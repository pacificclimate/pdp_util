from urllib import urlencode
from datetime import datetime

import pytest
from webob.request import Request
import json

from pdp_util.counts import CountStationsApp, CountRecordLengthApp

@pytest.mark.parametrize(('filters', 'expected'), [
    ({'network-name': 'EC_raw'}, 4),
    ({'from-date': '2000/01/01', 'to-date': '2000/01/31'}, 14),
    ({'to-date': '1965/01/01'}, 3),
    ({'input-freq': '1-hourly'}, 3),
    ({'only-with-climatology': 'only-with-climatology'}, 14),
    # We _should_ ignore a bad value for a filter (or return a HTTP BadRequest?)
    ({'only-with-climatology': 'bad-value'}, 50)
# Omit this case until we get the geoalchemy stuff figured out
#({'input-polygon': 'POLYGON((-123.240336 50.074796,-122.443323 49.762922,-121.992837 49.416394,-122.235407 48.654034,-123.725474 48.792645,-123.864085 49.728269,-123.240336 50.074796))'}, 7),
    ])
def test_count_stations_app(conn_params, filters, expected):
    app = CountStationsApp(conn_params)
    req = Request.blank('?' + urlencode(filters))
    resp = req.get_response(app)
    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    assert 'stations_selected' in resp.body
    data = json.loads(resp.body)
    assert data['stations_selected'] == expected

def test_count_record_length_app(conn_params):
    app = CountRecordLengthApp(conn_params, 3000)
    req = Request.blank('')
    resp = req.get_response(app)

    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    assert 'record_length' in resp.body
    assert 'climo_length' in resp.body
    
    data = json.loads(resp.body)
    assert 'record_length' in data
    assert 'climo_length' in data
    assert data['record_length'] == 2045
    assert data['climo_length'] == 360


def test_filter_dates_on_record_length_app(conn_params):
    app = CountRecordLengthApp(conn_params, 3000)

    # Get the base number of records
    req = Request.blank('')
    base_number = json.loads(req.get_response(app).body)

    # Get the filtered response
    sdate, edate = datetime(2000, 01, 01), datetime(2000, 01, 31)

    param_sets = [
        {'from-date': sdate.strftime('%Y/%m/%d'),
         'to-date': edate.strftime('%Y/%m/%d'),
         'answer': 0},
        {'from-date': sdate.strftime('%Y/%m/%d'),
         'answer': 0},
        {'to-date': edate.strftime('%Y/%m/%d'),
         'answer': 0},
        {'from-date': sdate.strftime('%Y/%m/%d'),
         'to-date': edate.strftime('%Y/%m/%d'),
         'cliptodate': 'True',
         'answer': 0},
        {'from-date': sdate.strftime('%Y/%m/%d'),
         'cliptodate': 'doit',
         'answer': 0},
        {'to-date': edate.strftime('%Y/%m/%d'),
         'cliptodate': 'I do believe so',
         'answer': 0}
        ]

    for params in param_sets:
        req = Request.blank('?' + urlencode(params))
        resp = req.get_response(app)

        assert resp.status == '200 OK'
        assert resp.content_type == 'application/json'
        assert 'record_length' in resp.body
        assert 'climo_length' in resp.body
    
        data = json.loads(resp.body)
        assert 'record_length' in data
        assert 'climo_length' in data
        assert data['record_length'] > 0
        assert data['climo_length'] > 0

        assert data['record_length'] < base_number['record_length']
        # Climatologies aren't filtered by date, only station

def test_length_of_return_dataset(test_session):
    pass
