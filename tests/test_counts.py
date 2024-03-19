from urllib.parse import urlencode
from datetime import datetime

import pytest
from sqlalchemy.dialects import postgresql
from pycds import CrmpNetworkGeoserver
from webob.request import Request
import json

from pdp_util.counts import CountStationsApp, CountRecordLengthApp


def cng_query(
    sesh,
    title="foo",
    filts=tuple(),
    cols=("station_id", "network_name", "native_id"),
):
    q = sesh.query(CrmpNetworkGeoserver).order_by(
        CrmpNetworkGeoserver.network_name, CrmpNetworkGeoserver.native_id
    )
    for f in filts:
        q = q.filter(f)
    rows = q.all()

    print()
    print(f"### cng: {title}; count {len(rows)}")
    print(cols)
    for row in rows:
        print(*(getattr(row, col) for col in cols))


@pytest.mark.skip
def test_database_contents(test_session):
    """Test the test_session database contents directly, for comparison against
    the results of CountStationsApp"""
    cng_query(
        test_session,
        title="all",
        cols=(
            "station_id",
            "network_name",
            "native_id",
            "min_obs_time",
            "max_obs_time",
            "unique_variable_tags",
        ),
    )
    cng_query(
        test_session,
        title="network-name == EC_raw",
        filts=(CrmpNetworkGeoserver.network_name == "EC_raw",),
        cols=("station_id", "network_name", "native_id"),
    )
    cng_query(
        test_session,
        title="from-date 2000/01/01, to-date 2000/01/31",
        filts=(
            CrmpNetworkGeoserver.max_obs_time > datetime(2000, 1, 1),
            CrmpNetworkGeoserver.min_obs_time < datetime(2000, 1, 31),
        ),
        cols=(
            "station_id",
            "network_name",
            "native_id",
            "min_obs_time",
            "max_obs_time",
        ),
    )
    cng_query(
        test_session,
        title="to-date 1965/01/01",
        filts=(CrmpNetworkGeoserver.min_obs_time < datetime(1965, 1, 1),),
        cols=(
            "station_id",
            "network_name",
            "native_id",
            "min_obs_time",
            "max_obs_time",
        ),
    )
    cng_query(
        test_session,
        title="input-freq 1-hourly",
        filts=(CrmpNetworkGeoserver.freq == "1-hourly",),
        cols=("station_id", "network_name", "native_id", "freq"),
    )
    cng_query(
        test_session,
        title="only-with-climatology",
        filts=(
            CrmpNetworkGeoserver.unique_variable_tags.contains(
                postgresql.array(["climatology"])
            ),
        ),
        cols=("station_id", "network_name", "native_id", "unique_variable_tags"),
    )


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"network-name": "EC_raw"}, 3),
        ({"from-date": "2000/01/01", "to-date": "2000/01/31"}, 11),
        ({"to-date": "1965/01/01"}, 2),
        ({"input-freq": "1-hourly"}, 6),
        ({"only-with-climatology": "only-with-climatology"}, 12),
        # We _should_ ignore a bad value for a filter (or return a HTTP BadRequest?)
        ({"only-with-climatology": "bad-value"}, 50),
        (
            {
                "input-polygon": "POLYGON ((-123.240336 50.074796,-122.443323 49.762922,-121.992837 49.416394,-122.235407 48.654034,-123.725474 48.792645,-123.864085 49.728269,-123.240336 50.074796))"
            },
            4,
        ),
        (
            {
                "input-polygon": "MULTIPOLYGON (((-123.240336 50.074796,-122.443323 49.762922,-121.992837 49.416394,-122.235407 48.654034,-123.725474 48.792645,-123.864085 49.728269,-123.240336 50.074796)))"
            },
            4,
        ),
    ],
)
def test_count_stations_app(test_session, filters, expected):
    app = CountStationsApp()
    req = Request.blank("?" + urlencode(filters), {"sesh": test_session})
    resp = req.get_response(app)
    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"
    assert "stations_selected" in resp.app_iter
    data = json.loads(resp.app_iter)
    assert data["stations_selected"] == expected


def test_count_record_length_app(test_session):
    app = CountRecordLengthApp(None, 3000)
    req = Request.blank("", {"sesh": test_session})
    resp = req.get_response(app)

    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"
    assert "record_length" in resp.app_iter
    assert "climo_length" in resp.app_iter

    data = json.loads(resp.app_iter)
    assert "record_length" in data
    assert "climo_length" in data
    assert data["record_length"] == 1969
    assert data["climo_length"] == 412


sdate, edate = datetime(2000, 1, 1), datetime(2000, 1, 31)


@pytest.mark.parametrize(
    ("params"),
    (
        {
            "from-date": sdate.strftime("%Y/%m/%d"),
            "to-date": edate.strftime("%Y/%m/%d"),
        },
        {"from-date": sdate.strftime("%Y/%m/%d")},
        {"to-date": edate.strftime("%Y/%m/%d")},
        {
            "from-date": sdate.strftime("%Y/%m/%d"),
            "to-date": edate.strftime("%Y/%m/%d"),
            "cliptodate": "True",
        },
        {"from-date": sdate.strftime("%Y/%m/%d"), "cliptodate": "doit"},
        {"to-date": edate.strftime("%Y/%m/%d"), "cliptodate": "I do believe so"},
    ),
)
def test_filter_dates_on_record_length_app(test_session, params):
    app = CountRecordLengthApp(None, 3000)

    # Get the base number of records
    req = Request.blank("", {"sesh": test_session})
    base_number = json.loads(req.get_response(app).app_iter)

    req = Request.blank("?" + urlencode(params), {"sesh": test_session})
    resp = req.get_response(app)

    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"
    assert "record_length" in resp.app_iter
    assert "climo_length" in resp.app_iter

    data = json.loads(resp.app_iter)
    assert "record_length" in data
    assert "climo_length" in data
    assert data["record_length"] > 0
    assert data["climo_length"] > 0

    assert data["record_length"] < base_number["record_length"]
    # Climatologies aren't filtered by date, only station


def test_length_of_return_dataset(test_session):
    pass
