from os.path import dirname

from webob.request import Request
from bs4 import BeautifulSoup


from pdp_util.pcds_index import PcdsIsClimoIndex, PcdsNetworkIndex, PcdsStationIndex
from pycds import *


def make_common_assertions(resp):
    assert resp.status == "200 OK"
    assert resp.content_type == "text/html"
    assert resp.content_length is None


def test_climo_index(pkg_file_root, conn_params, test_session):
    app = PcdsIsClimoIndex(
        app_root="/",
        templates=str(pkg_file_root("pdp_util") / "templates"),
        conn_params=conn_params,
    )  # FIXME: template path is fragile

    assert app.get_elements(test_session) == (
        ("climo", "Climatological calculations"),
        ("raw", "Raw measurements from participating networks"),
    )

    req = Request.blank("")
    resp = req.get_response(app)
    make_common_assertions(resp)

    assert "Climatological calculations" in str(resp.app_iter)
    assert "raw/" in str(resp.app_iter)


def test_network_index(pkg_file_root, conn_params, test_session):
    app = PcdsNetworkIndex(
        app_root="/",
        templates=str(pkg_file_root("pdp_util") / "templates"),
        conn_params=conn_params,
        is_climo=False,
    )  # FIXME: template path is fragile

    assert set(app.get_elements(test_session)) == {
        ("AGRI", "BC Ministry of Agriculture"),
        ("ARDA", "Agricultural and Rural Development Act Network"),
        ("EC", "Environment Canada (Canadian Daily Climate Data 2007)"),
        ("EC_raw", 'Environment Canada (raw observations from "Climate Data Online")'),
        ("ENV-AQN", "BC Ministry of Environment - Air Quality Network"),
        (
            "FLNRO-WMB",
            "BC Ministry of Forests, Lands, and Natural Resource Operations - Wild Fire Managment Branch",
        ),
        ("MoTIe", "Ministry of Transportation and Infrastructure (electronic)"),
    }

    req = Request.blank("/pcds/raw/", {"sesh": test_session})
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")

    assert "Participating CRMP Networks" in soup.title.string
    assert "FLNRO-WMB/" in str(resp.app_iter)
    assert "Environment Canada (Canadian Daily Climate Data 2007)" in str(resp.app_iter)


def test_station_index(pkg_file_root, conn_params, test_session):
    app = PcdsStationIndex(
        app_root="/",
        templates=str(pkg_file_root("pdp_util") / "templates"),
        conn_params=conn_params,
        is_climo=False,
        network="AGRI",
    )  # FIXME: template path is fragile

    assert app.get_elements(test_session) == [("de107", "Deep Creek")]

    req = Request.blank("/pcds/raw/AGRI/", {"sesh": test_session})
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.app_iter, features="html.parser")

    assert "Stations for network AGRI" in soup.title.string
    assert "de107/" in str(resp.app_iter)
    assert "Deep Creek" in str(resp.app_iter)

    assert "de107climo/" not in str(resp.app_iter)
    assert 'Deep "Climo Station" Creek' not in str(resp.app_iter)


def test_station_index_for_climatologies(pkg_file_root, conn_params, test_session):
    app = PcdsStationIndex(
        app_root="/",
        templates=str(pkg_file_root("pdp_util") / "templates"),
        conn_params=conn_params,
        is_climo=True,
        network="MoTIe",
    )

    assert app.get_elements(test_session) == [("34129", "London Ridge High")]

    req = Request.blank("/pcds/climo/MoTIe/", {"sesh": test_session})
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(str(resp.app_iter), features="html.parser")

    assert "Stations for network MoTIe" in soup.title.string
    assert "15124/" not in str(resp.app_iter)
    assert "Jackass" not in str(resp.app_iter)
    assert "34129/" in str(resp.app_iter)
    assert "London Ridge High" in str(resp.app_iter)
