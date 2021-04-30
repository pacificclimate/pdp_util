from pdp_util.raster import (
    ensemble_files,
    db_raster_catalog,
    db_raster_configurator,
    EnsembleCatalog,
    RasterServer,
)
import json

import pytest
from webob.request import Request

import pytest
import requests
from urllib.request import urlretrieve
from tempfile import NamedTemporaryFile
from webob.response import Response
from netCDF4 import Dataset


@pytest.fixture(scope="function")
def config():
    return {"ensemble": "bc_prism", "root_url": "http://basalt.pcic.uvic.ca:8080/data/"}


def test_ensemble_files(mm_session):
    result = ensemble_files(mm_session, "bc_prism")
    assert result == {
        "tmax_monClim_PRISM_historical_run1_197101-200012": "/home/data/climate/PRISM/dataportal/tmax_monClim_PRISM_historical_run1_197101-200012.nc",
        "pr_monClim_PRISM_historical_run1_197101-200012": "/home/data/climate/PRISM/dataportal/pr_monClim_PRISM_historical_run1_197101-200012.nc",
        "tmin_monClim_PRISM_historical_run1_197101-200012": "/home/data/climate/PRISM/dataportal/tmin_monClim_PRISM_historical_run1_197101-200012.nc",
    }


def test_db_raster_catalog(mm_session, config):
    result = db_raster_catalog(mm_session, config["ensemble"], config["root_url"])
    assert result == {
        "tmax_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/tmax_monClim_PRISM_historical_run1_197101-200012.nc",
        "pr_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/pr_monClim_PRISM_historical_run1_197101-200012.nc",
        "tmin_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/tmin_monClim_PRISM_historical_run1_197101-200012.nc",
    }


def test_db_raster_configurator(mm_session):
    args = [
        mm_session,
        "A name",
        "Version 0.0.0.1",
        "0.0",
        "bc_prism",
        "http://basalt.pcic.uvic.ca:8080/data/",
    ]
    result = db_raster_configurator(*args)
    assert all(
        [
            x in result.items()
            for x in {
                "root_url": "http://basalt.pcic.uvic.ca:8080/data/",
                "name": "A name",
                "version": "Version 0.0.0.1",
                "ensemble": "bc_prism",
                "api_version": "0.0",
            }.items()
        ]
    )


def test_db_raster_configurator_handlers(mm_session):
    # Handlers must be tested seperately because they are a list and order cannot be guaranteed on database query
    args = [
        mm_session,
        "A name",
        "Version 0.0.0.1",
        "0.0",
        "bc_prism",
        "http://basalt.pcic.uvic.ca:8080/data/",
    ]
    handlers = [
        {
            "url": "tmax_monClim_PRISM_historical_run1_197101-200012.nc",
            "file": "/home/data/climate/PRISM/dataportal/tmax_monClim_PRISM_historical_run1_197101-200012.nc",
        },
        {
            "url": "pr_monClim_PRISM_historical_run1_197101-200012.nc",
            "file": "/home/data/climate/PRISM/dataportal/pr_monClim_PRISM_historical_run1_197101-200012.nc",
        },
        {
            "url": "tmin_monClim_PRISM_historical_run1_197101-200012.nc",
            "file": "/home/data/climate/PRISM/dataportal/tmin_monClim_PRISM_historical_run1_197101-200012.nc",
        },
    ]
    result = db_raster_configurator(*args)
    assert all([x in result["handlers"] for x in handlers])


@pytest.mark.parametrize("url", ["", "/url/is/irrellevant"])
def test_ensemble_catalog(mm_dsn, config, url):
    app = EnsembleCatalog(mm_dsn, config)
    req = Request.blank(url)
    resp = req.get_response(app)
    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"
    body = json.loads(resp.app_iter[0])
    assert body == {
        "tmax_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/tmax_monClim_PRISM_historical_run1_197101-200012.nc",
        "pr_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/pr_monClim_PRISM_historical_run1_197101-200012.nc",
        "tmin_monClim_PRISM_historical_run1_197101-200012": "http://basalt.pcic.uvic.ca:8080/data/tmin_monClim_PRISM_historical_run1_197101-200012.nc",
    }


def test_raster_metadata_minmax(raster_metadata):
    req = Request.blank(
        "?request=GetMinMax&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax"
    )
    resp = req.get_response(raster_metadata)

    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"

    stats = json.loads(resp.app_iter)
    assert len(stats) == 2
    assert set(stats.keys()) == set(["max", "min"])


def test_raster_metadata_minmax_w_units(raster_metadata):
    req = Request.blank(
        "?request=GetMinMaxWithUnits&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax"
    )
    resp = req.get_response(raster_metadata)

    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"

    stats = json.loads(resp.app_iter)
    assert len(stats) == 3
    assert set(stats.keys()) == set(["max", "min", "units"])


def test_raster_metadata_minmax_no_id(raster_metadata):
    req = Request.blank(
        "?request=INVALID_REQUEST_TYPE&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax"
    )
    resp = req.get_response(raster_metadata)
    assert resp.status == "400 Bad Request"


def test_raster_metadata_minmax_no_id(raster_metadata):
    req = Request.blank("?request=GetMinMax&var=tasmax")
    resp = req.get_response(raster_metadata)
    assert resp.status == "400 Bad Request"


def test_raster_metadata_minmax_no_var(raster_metadata):
    req = Request.blank(
        "?request=GetMinMax&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231"
    )
    resp = req.get_response(raster_metadata)
    assert resp.status == "400 Bad Request"


def test_raster_metadata_minmax_bad_id(raster_metadata):
    req = Request.blank("?request=GetMinMax&id=NOT_A_VALID_ID&var=tasmax")
    resp = req.get_response(raster_metadata)
    assert resp.status == "404 Not Found"


@pytest.mark.online
@pytest.mark.parametrize(
    ("environ", "var"),
    [
        (
            {
                "PATH_INFO": "tasmin_day_BCSD+ANUSPLIN300+GFDL-ESM2G_historical+rcp26_r1i1p1_19500101-21001231.nc.nc",
                "QUERY_STRING": "tasmin[0:150][0:91][0:206]&",
            },
            "tasmin",
        ),
        (
            {
                "PATH_INFO": "pr_day_BCCAQv2+ANUSPLIN300_NorESM1-ME_historical+rcp85_r1i1p1_19500101-21001231.nc.nc",
                "QUERY_STRING": "pr[0:500][91:91][206:206]&",
            },
            "pr",
        ),
    ],
)
@pytest.mark.online
@pytest.mark.parametrize(
    ("config"),
    [
        {
            "root_url": "http://tools.pacificclimate.org/dataportal/data/vic_gen1/",
            "handlers": [
                {
                    "url": "tasmin_day_BCSD+ANUSPLIN300+GFDL-ESM2G_historical+rcp26_r1i1p1_19500101-21001231.nc",
                    "file": "/storage/data/climate/downscale/BCCAQ2/bccaqv2_with_metadata/tasmin_day_BCCAQv2+ANUSPLIN300_inmcm4_historical+rcp85_r1i1p1_19500101-21001231.nc",
                },
                {
                    "url": "pr_day_BCCAQv2+ANUSPLIN300_NorESM1-ME_historical+rcp85_r1i1p1_19500101-21001231.nc",
                    "file": "/storage/data/climate/downscale/BCCAQ2/bccaqv2_with_metadata/pr_day_BCCAQv2+ANUSPLIN300_NorESM1-ME_historical+rcp85_r1i1p1_19500101-21001231.nc",
                },
            ],
            "thredds_root": "http://docker-dev03.pcic.uvic.ca:30333/data",
        }
    ],
)
def test_RasterServer_orca(mm_database_dsn, config, environ, var):
    r_server = RasterServer(mm_database_dsn, config)
    resp = r_server(environ, Response())

    assert resp.status_code == 301

    r = requests.get(resp.location, allow_redirects=True)
    with NamedTemporaryFile(suffix=".nc", dir="/tmp") as tmp_file:
        urlretrieve(r.url, tmp_file.name)
        data = Dataset(tmp_file.name)
        assert "time" in data.dimensions
        assert "lat" in data.dimensions
        assert "lon" in data.dimensions
        assert var in data.variables


@pytest.mark.online
@pytest.mark.parametrize(
    ("environ", "config"),
    [
        (
            {
                "PATH_INFO": "bad_path.nc.nc",
                "QUERY_STRING": "tasmin[0:150][0:91][0:206]&",
            },
            {
                "root_url": "http://tools.pacificclimate.org/dataportal/data/vic_gen1/",
                "handlers": [
                    {"url": "bad_path.nc", "file": "bad_file.nc"},
                ],
                "thredds_root": "http://docker-dev03.pcic.uvic.ca:30333/data",
            },
        )
    ],
)
def test_RasterServer_orca_error(mm_database_dsn, config, environ):
    r_server = RasterServer(mm_database_dsn, config)
    resp = r_server(environ, Response())

    r = requests.get(resp.location, allow_redirects=True)
    assert "Server Error" in str(r.content)
