import pytest
import urllib
from os.path import basename
from pdp_util.raster import (
    ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog, RasterServer
)
from tempfile import NamedTemporaryFile
from webob.response import Response
from netCDF4 import Dataset


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
def test_ensemble_files(mm_test_session, ensemble):
    result = ensemble_files(mm_test_session, ensemble.name)
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert result == {df.unique_id: df.filename for df in data_files}


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize("root_url", ["foo/", "bar/"])
def test_db_raster_catalog(mm_test_session, ensemble, root_url):
    result = db_raster_catalog(mm_test_session, ensemble.name, root_url)
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert result == {
        df.unique_id: f"{root_url}{basename(df.filename)}" for df in data_files
    }


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize(
    "name, version, api_version, root_url",
    [
        ("A name", "Version 0.0.0.1", "0.0", "http://example.com/"),
    ],
)
def test_db_raster_configurator(
    mm_test_session,
    name,
    version,
    api_version,
    ensemble,
    root_url,
):
    result = db_raster_configurator(
        mm_test_session,
        name,
        version,
        api_version,
        ensemble.name,
        root_url,
    )

    for x in {
        "root_url": root_url,
        "name": name,
        "version": version,
        "ensemble": ensemble.name,
        "api_version": api_version,
    }.items():
        assert x in result.items()
    data_files = {dfv.file for dfv in ensemble.data_file_variables}

    handlers = [
        {"url": basename(df.filename), "file": df.filename} for df in data_files
    ]
    assert len(result["handlers"]) == len(handlers)
    for x in result["handlers"]:
        assert x in handlers


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize("url", ["", "/url/is/irrelevant"])
@pytest.mark.parametrize("root_url", ["http://example.com/"])
@pytest.mark.usefixtures("mm_test_session_committed")
def test_ensemble_catalog(mm_database_dsn, test_wsgi_app, ensemble, root_url, url):
    config = {
        "ensemble": ensemble.name,
        "root_url": root_url,
    }
    app = EnsembleCatalog(mm_database_dsn, config)
    resp, body = test_wsgi_app(app, url, "200 OK", "application/json")
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert body == {
        df.unique_id: f"{root_url}{basename(df.filename)}" for df in data_files
    }


# Valid request parameters with a variety of id, var cases.
@pytest.mark.parametrize(
    "req, req_keys",
    [
        (None, {"min", "max"}),
        ("GetMinMax", {"min", "max"}),
        ("GetMinMaxWithUnits", {"min", "max", "units"}),
    ],
)
@pytest.mark.parametrize(
    "include, inc_keys",
    [
        (None, {"min", "max"}),
        ("units", {"min", "max", "units"}),
        ("filepath", {"min", "max", "filepath"}),
        ("filepath,units", {"min", "max", "filepath", "units"}),
        ("units,filepath", {"min", "max", "filepath", "units"}),
    ],
)
@pytest.mark.parametrize(
    "id_, var, status, content_type",
    [
        ("unique_id_0", "var_0", "200 OK", "application/json"),
        ("unique_id_0", "var_1", "200 OK", "application/json"),
        ("unique_id_1", "var_2", "200 OK", "application/json"),
        ("unique_id_2", "var_3", "200 OK", "application/json"),
        ("unique_id_1", "var_0", "404 Not Found", "application/json"),
        ("unique_id_0", "wrong", "404 Not Found", "application/json"),
        ("wrong", "var_0", "404 Not Found", "application/json"),
        (None, "var_0", "400 Bad Request", None),
        ("unique_id_0", None, "400 Bad Request", None),
        (None, None, "400 Bad Request", None),
    ],
)
@pytest.mark.usefixtures("mm_test_session_committed")
def test_raster_metadata_minmax(
    raster_metadata,
    test_wsgi_app,
    query_params,
    req,
    req_keys,
    include,
    inc_keys,
    id_,
    var,
    status,
    content_type,
):
    url = query_params(
        ("request", req),
        ("include", include),
        ("id", id_),
        ("var", var),
    )
    resp, body = test_wsgi_app(raster_metadata, url, status, content_type)
    keys = req_keys | inc_keys
    if status[:3] == "200":
        assert set(body.keys()) == keys


# Tests that don't fit the parametrization pattern above.


@pytest.mark.parametrize(
    "req, include",
    [
        ("invalid", None),
        (None, "invalid"),
        (None, "units,invalid"),
        ("invalid", "invalid"),
    ],
)
@pytest.mark.usefixtures("mm_test_session_committed")
def test_raster_metadata_minmax_bad_params(
    raster_metadata, test_wsgi_app, query_params, req, include
):
    url = query_params(
        ("request", req), ("include", include), ("id", "unique_id_1"), ("var", "var_2")
    )
    test_wsgi_app(raster_metadata, url, '400 Bad Request', None)


@pytest.mark.online
@pytest.mark.parametrize(
    ("config", "environ"),
    [
        (
            {
                'root_url': 'http://tools.pacificclimate.org/dataportal/data/vic_gen1/',
                'name': 'testing-server',
                'version': 0,
                'api_version': 0,
                'handlers': [
                    {
                        'url': 'pr+tasmax+tasmin_day_BCSD+ANUSPLIN300+GFDL-ESM2G_historical+rcp26_r1i1p1_19500101-21001231.nc',
                        'file': '/storage/data/climate/downscale/CMIP5/BCSD/pr+tasmax+tasmin_day_BCSD+ANUSPLIN300+GFDL-ESM2G_historical+rcp26_r1i1p1_19500101-21001231.nc'
                    },
                    {
                        'url': 'pr+tasmin+tasmax+wind_day_CGCM3_A1B_run1_19500101-21001231.nc',
                        'file': '/storage/data/projects/dataportal/data/vic_gen1_input/pr+tasmin+tasmax+wind_day_CGCM3_A1B_run1_19500101-21001231.nc'
                    },
                ],
                'ensemble' : 'bccaq2',
                'thredds_root' : 'http://docker-dev03.pcic.uvic.ca:30333'
            },
            {
                'PATH_INFO' : 'pr+tasmax+tasmin_day_BCSD+ANUSPLIN300+GFDL-ESM2G_historical+rcp26_r1i1p1_19500101-21001231.nc.nc',
                'QUERY_STRING' : 'tasmax[0:150][0:91][0:206]&',
}
        )
    ]
)
def test_RasterServer_orca(mm_database_dsn, config, environ):
    r_server = RasterServer(mm_database_dsn, config)
    resp = r_server(environ, Response())

    print(resp.location)

    assert resp.status_code == 301
    assert config['thredds_root'] in resp.location
    with NamedTemporaryFile(suffix=".nc", dir="/tmp") as tmp_file:
        filename = urllib.urlretrieve(resp.location, tmp_file.name)
        with Dataset(filename) as data:
            assert len(data.variable) > 0
