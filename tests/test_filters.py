from datetime import datetime
from urllib import urlencode

from pdp_util.filters import form_filters, validate_vars
from pycds import CrmpNetworkGeoserver as cng

import pytest
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.dialects import postgresql
from sqlalchemy import func, or_
from webob.request import Request


def render(expression):
    # Return a string representing `expression`, with literals bound.
    # Use PostgreSQL dialect.
    return str(expression.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"literal_binds": True}
    ))


def test_can_instantiate_filters():
    assert isinstance(form_filters['from-date']('2010/01/01'), ColumnElement)
    #assert isinstance(form_filters['input-polygon'](''), sqlalchemy.sql.expression.BinaryExpression)

@pytest.mark.parametrize(('input', 'expected'), [
    # No filters
    ({}, []),

    # Valid values for each filter, singly
    ({'from-date': '2000/01/01'}, [ cng.max_obs_time > datetime(2000, 1, 1) ]),

    ({'to-date': '2000/01/01'}, [ cng.min_obs_time < datetime(2000, 1, 1) ]),

    ({'network-name': ''}, [ True ]),
    ({'network-name': 'EC_raw'}, [ cng.network_name.in_(['EC_raw']) ]),
    ({'network-name': 'EC_raw,ARDA'}, [ cng.network_name.in_(['EC_raw', 'ARDA']) ]),

    ({'input-var': 'variable'}, [ cng.vars.like('%variable%') ]), # OK

    ({'input-vars': ''}, [ True ]),
    ({'input-vars': 'var1'}, [
        func.regexp_split_to_array(cng.vars, ',\\s*')
            .overlap(postgresql.array(['var1']))
    ]),
    ({'input-vars': 'var1,var2'}, [
        func.regexp_split_to_array(cng.vars, ',\\s*')
            .overlap(postgresql.array(['var1', 'var2']))
    ]),

    ({'input-freq': ''}, [ True ]),
    ({'input-freq': '1-hourly'}, [ cng.freq.in_(['1-hourly']) ]),
    ({'input-freq': '1-hourly,irregular'}, [ cng.freq.in_(['1-hourly', 'irregular']) ]),

    ({'only-with-climatology': 'only-with-climatology'}, [ or_(cng.vars.like('%within%'), cng.vars.like('%over%')) ]),

    # Invalid values for each filter, singly
    ({'from-date': '2000/Jan/01'}, []),
    ({'to-date': '2000/Jan/01'}, []),
    ({'network-name': '%argle&'}, []),
    ({'input-var': '%&Invalid variable! name!'}, []),
    ({'input-vars': 'blerg!,&frabble'}, []),
    ({'input-freq': 'infrequent'}, []),
    ({'only-with-climatology': 'invalid value'}, []),

    # One valid and one invalid filter
    ({'to-date': '2000/01/01', 'non-existant-filter': 'bar'}, [  cng.min_obs_time < datetime(2000, 1, 1) ]),

    # Several valid filters
    ({'input-freq': 'irregular', 'only-with-climatology': 'only-with-climatology', 'from-date': '1890/12/25'},
     [cng.freq.in_(['irregular']), or_(cng.vars.like('%within%'), cng.vars.like('%over%')), cng.max_obs_time > datetime(1890, 12, 25)]),
])
def test_validate_vars(input, expected):
    req = Request.blank('?' + urlencode(input))
    result = validate_vars(req.environ)

    if result and expected:
        # Ordering is not guaranteed for results (since the order of filters
        # is irrelevant) Could do a complicated set compare, but just do an
        # O(n**2) comparision for simplicity.
        # make a copy of both lists since we'll be mutating them in loop
        for a in result[:]:
            for b in expected[:]:
                if a == b or (isinstance(a, ColumnElement) and a.compare(b)):
                    result.remove(a)
                    expected.remove(b)
                else:
                    # `a.compare(b)` returns `False` when `a` and `b` are
                    # actually different _or_ when the comparison can't be
                    # performed. (This is the case for PostgreSQL dialect-
                    # specific functions such as `regexp_split_to_array`.)
                    #
                    # Therefore, on `False`, try comparing the SQL that `a` and
                    # `b` render to. `render` can fail, but as it turns out
                    # only for cases where the expressions are actually
                    # different (and hence not a problem for the overall
                    # comparison of `result` and `expected`). This is a bit
                    # fragile, but it is sufficient for present purposes.
                    try:
                        if render(a) == render(b):
                            result.remove(a)
                            expected.remove(b)
                    except NotImplementedError:
                        pass
        assert expected == []
        assert result == []
