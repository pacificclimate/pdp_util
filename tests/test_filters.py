from datetime import datetime
from urllib import urlencode

from pdp_util.filters import form_filters, validate_vars
from pycds import CrmpNetworkGeoserver as cng

import pytest
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy import or_
from webob.request import Request

def test_can_instantiate_filters():
    assert isinstance(form_filters['from-date']('2010/01/01'), ColumnElement)
    #assert isinstance(form_filters['input-polygon'](''), sqlalchemy.sql.expression.BinaryExpression)

@pytest.mark.parametrize(('input', 'expected'), [
    # No valid filters
    ({}, []),
    # One valid filter
    ({'network-name': 'EC_raw'}, [ cng.network_name == 'EC_raw' ]),
    # One valid and one invalid filter
    ({'to-date': '2000/01/01', 'non-existant-filter': 'bar'}, [  cng.min_obs_time < datetime(2000, 1, 1) ]),
    # Several valid filters
    ({'input-freq': 'irregular', 'only-with-climatology': 'only-with-climatology', 'from-date': '1890/12/25'},
     [cng.freq == 'irregular', or_(cng.vars.like('%within%'), cng.vars.like('%over%')), cng.max_obs_time > datetime(1890, 12, 25)]),
    # valid filter names with invalid data
    ({'input-var': '%&Invalid variable! name!'}, []),
    ({'only-with-climatology': 'invalid value'}, [])
    ])
def test_validate_vars(input, expected):
    req = Request.blank('?' + urlencode(input))
    result = validate_vars(req.environ)

    if result and expected:
        # Ordering is not guaranteed for results (since the order of filters is irrelevant)
        # Could do a complicated set compare, but just do an n**2 comparision for simplicity
        for a in result[:]: # make a copy of both lists since we'll be mutating them in loop
            for b in expected[:]:
                if isinstance(a, ColumnElement):
                    #assert False
                    if a.compare(b):
                        result.remove(a)
                        expected.remove(b)
                else:
                    if a == b:
                        result.remove(a)
                        expected.remove(b)
        assert expected == []
        assert result == []
