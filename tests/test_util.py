from urllib import urlencode
from datetime import datetime

from pycds import Network, CrmpNetworkGeoserver as cng
from pdp_util.util import get_stn_list, get_clip_dates, get_extension

import pytest
from webob.request import Request

def test_get_stn_list(test_session_with_data):
    stns = get_stn_list(test_session_with_data, [])
    assert len(stns) == 50

@pytest.mark.parametrize(('constraints', 'to_select', 'expected'), [
    ([Network.name == 'EC_raw'],
     cng.native_id,
     ['1046332', '1126150', '1106200', '1022795']
    ),
    ([cng.max_obs_time > datetime(2000, 1, 1), cng.min_obs_time < datetime(2000, 1, 31)],
     cng.station_id,
     [613, 813, 913, 1113, 1413, 1613, 1713, 1813, 1913, 2113, 2213, 2513, 2773, 3084]),
    ([cng.min_obs_time < datetime(1965, 1, 1)],
     cng.station_id,
     [513, 613, 813]),
    ([cng.freq == '1-hourly'],
     cng.network_name,
     ['FLNRO-WMB', 'MoTIe', 'EC_raw'])
    ])
def test_get_stn_list_with_filter(test_session_with_data, constraints, to_select, expected):
    stns = get_stn_list(test_session_with_data, constraints, to_select)
    assert set(expected) == set([x[0] for x in stns])
    
def test_single_column_select(test_session_with_data):
    stns = get_stn_list(test_session_with_data, [], cng.station_id)
    assert isinstance(stns[0][0], int)

def test_get_clip_dates():
    sdate, edate = datetime(2000, 01, 01), datetime(2000, 01, 31)
    params = {'from-date': sdate.strftime('%Y/%m/%d'),
               'to-date': edate.strftime('%Y/%m/%d')}
    req = Request.blank('?' + urlencode(params))
    rv = get_clip_dates(req.environ)
    # If cliptodate is not set, then get_clip_dates ignores the dates
    assert rv == (None, None)
    
    params['cliptodate'] = 'True'
    req = Request.blank('?' + urlencode(params))
    rv = get_clip_dates(req.environ)
    assert rv == (sdate, edate)

    # Does it work with just one of the two dates?
    del params['from-date']
    req = Request.blank('?' + urlencode(params))
    rv = get_clip_dates(req.environ)
    assert rv == (None, edate)

def test_get_extension_good():
    params = {'data-format': 'csv'}
    req = Request.blank('?' + urlencode(params))
    assert get_extension(req.environ) == 'csv'

def test_get_extension_bad():
    params = {'data-format': 'unsupported_extension'}
    req = Request.blank('?' + urlencode(params))
    assert get_extension(req.environ) == None

    # data-format not in the request params
    req = Request.blank('')
    assert get_extension(req.environ) == None
