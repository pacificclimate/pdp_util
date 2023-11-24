
from pdp_util.pcds_dispatch import PcdsDispatcher
from pydap_extras.handlers.pcic import RawPcicSqlHandler, ClimoPcicSqlHandler
# from pycds.util import sql_station_table

import pytest
from webob.request import Request
from bs4 import BeautifulSoup


def make_common_assertions(resp):
    assert resp.status == "200 OK"
    assert resp.content_type == "text/html"
    if resp.content_length:
        assert resp.content_length > 0


@pytest.fixture(scope="function")  # TODO: Broaden scope?
def the_app(conn_params, pkg_file_root):
    kwargs = {
        "pydap_root": "/tmp/",
        "app_root": "/",
        "templates": str(pkg_file_root("pdp_util") / "templates"),
        "ol_path": "",
        "conn_params": conn_params,
    }
    return PcdsDispatcher(**kwargs)


def test_can_initialize(the_app):
    assert True  # We have the fixture, so clearly this is true


def test_climo_listing(the_app, test_session):
    url = "/"
    req = Request.blank(url, {"sesh": test_session})
    resp = req.get_response(the_app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")
    assert b"Climatological calculations" in resp.app_iter
    assert b"raw/" in resp.app_iter
    assert soup.title.string == "PCDS: PCDS Data"


@pytest.mark.parametrize("url", ["/raw/", "/raw"])
def test_network_listing(the_app, test_session, url):
    req = Request.blank(url, {"sesh": test_session})
    resp = req.get_response(the_app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")
    assert b"PCDS: Participating CRMP Networks" in resp.app_iter
    for network in [
        b"EC",
        b"ENV-AQN",
        b"ARDA",
        b"EC_raw",
        b"FLNRO-WMB",
        b"AGRI",
        b"MoTIe",
    ]:
        assert network in resp.app_iter
    assert soup.title.string == "PCDS: Participating CRMP Networks"
    assert b"Environment Canada (Canadian Daily Climate Data 2007)" in resp.app_iter


def test_bad_is_climo(the_app, test_session):
    url = "/unraw/"
    req = Request.blank(url, {"sesh": test_session})
    resp = req.get_response(the_app)
    assert resp.status == "404 Not Found"


@pytest.mark.parametrize("url", ["/raw/EC_raw"])
def test_station_listing(the_app, test_session, url):
    req = Request.blank(url, environ={"sesh": test_session})
    resp = req.get_response(the_app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")
    for station_name in [b"1106200", b"1046332", b"1126150"]:
        assert station_name in resp.app_iter
        soup.title.string == "PCIC Data Portal: Stations for network EC_raw"


def test_bad_network(the_app, test_session):
    url = "/raw/network_does_not_exist/"
    req = Request.blank(url, {"sesh": test_session})
    resp = req.get_response(the_app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")
    assert soup.title.string == "PCDS: Stations for network network_does_not_exist"

    stuff = soup.find_all("tr")
    assert len(stuff) == 1  # No stations to list


# Check the content is more of an integration test
# For the unit test, we should only check that it routes to a PcicSqlHandler
@pytest.mark.parametrize("url", ["/raw/EC/1106200/", "/raw/EC/1106200"])
def dont_test_station_listing(the_app, test_session, url, monkeypatch):
    def my_get_full_query(self, stn_id, sesh):
        return

    monkeypatch.setattr(RawPcicSqlHandler, "get_full_query", my_get_full_query)

    req = Request.blank(url)
    resp = req.get_response(the_app)
    make_common_assertions(resp)


def test_dispatch_to_station_listing(the_app, test_session):
    path = "/raw/EC/1106200/"
    cls_, args, kwargs, new_env = the_app._route_request(path, {"sesh": test_session})
    assert cls_ == RawPcicSqlHandler
    assert isinstance(cls_(*args, **kwargs), cls_)
    assert new_env["PATH_INFO"] == "/raw/EC/1106200.rsql.html"

    path = "/climo/EC/1106200/"
    cls_, args, kwargs, new_env = the_app._route_request(path, {"sesh": test_session})
    assert cls_ == ClimoPcicSqlHandler
    assert isinstance(cls_(*args, **kwargs), cls_)
    assert new_env["PATH_INFO"] == "/climo/EC/1106200.csql.html"


# FIXME: need to ORMify pydap_extras.handlers.pcic
def test_bad_station(the_app):
    url = "/raw/ARDA/non_existant_station"
    req = Request.blank(url)
    with pytest.raises(AssertionError):
        resp = req.get_response(the_app)


def dont_test_junk_at_the_end(the_app):
    url = "/raw/network_does_not_exist/plus_bad_station"
    req = Request.blank(url)
    resp = req.get_response(the_app)
    make_common_assertions(resp)
