from os.path import basename
from pdp_util.raster import ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
import json

import pytest
from webob.request import Request

@pytest.fixture(scope="function")
def config():
    return {'ensemble': 'bc_prism', 'root_url': 'http://basalt.pcic.uvic.ca:8080/data/'}


def test_ensemble1_files(mm_test_session, ensemble1, ensemble1_data_files):
    result = ensemble_files(mm_test_session, ensemble1.name)
    assert result == {df.unique_id: df.filename for df in ensemble1_data_files}


def test_ensemble2_files(mm_test_session, ensemble2, ensemble2_data_files):
    result = ensemble_files(mm_test_session, ensemble2.name)
    assert result == {df.unique_id: df.filename for df in ensemble2_data_files}


@pytest.mark.parametrize('root_url', ['foo/', 'bar/'])
def test_db_raster_catalog(
    mm_test_session, ensemble1, ensemble1_data_files, root_url
):
    result = db_raster_catalog(mm_test_session, ensemble1.name, root_url)
    assert result == {
        df.unique_id: '{}{}'.format(root_url, basename(df.filename))
        for df in ensemble1_data_files
    }


@pytest.mark.parametrize('name, version, api_version, ensemble, root_url', [
    ('A name', 'Version 0.0.0.1', '0.0', 'bc_prism', 'http://root.ca'),
])
def test_db_raster_configurator(
    mm_test_session, name, version, api_version, ensemble, root_url
):
    result = db_raster_configurator(
        mm_test_session, name, version, api_version, ensemble, root_url
    )
    assert all(
        x in result.items()
        for x in {
            'root_url': root_url,
            'name': name,
            'version': version,
            'ensemble': ensemble,
            'api_version': api_version,
        }.items()
    )


def test_db_raster_configurator_handlers(mm_session):
    # Handlers must be tested seperately because they are a list and order cannot be guaranteed on database query
    args = [mm_session, 'A name', 'Version 0.0.0.1', '0.0', 'bc_prism', 'http://basalt.pcic.uvic.ca:8080/data/']
    handlers = [{'url': 'tmax_monClim_PRISM_historical_run1_197101-200012.nc',
                 'file': '/home/data/climate/PRISM/dataportal/tmax_monClim_PRISM_historical_run1_197101-200012.nc'},
                {'url': 'pr_monClim_PRISM_historical_run1_197101-200012.nc',
                 'file': '/home/data/climate/PRISM/dataportal/pr_monClim_PRISM_historical_run1_197101-200012.nc'},
                {'url': 'tmin_monClim_PRISM_historical_run1_197101-200012.nc',
                 'file': '/home/data/climate/PRISM/dataportal/tmin_monClim_PRISM_historical_run1_197101-200012.nc'}]
    result = db_raster_configurator(*args)
    assert all([x in result['handlers'] for x in handlers])


@pytest.mark.parametrize('url', ['', '/url/is/irrellevant'])
def test_ensemble_catalog(mm_dsn, config, url):
    app = EnsembleCatalog(mm_dsn, config)
    req = Request.blank(url)
    resp = req.get_response(app)
    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    body = json.loads(resp.body)
    assert body == {'tmax_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/tmax_monClim_PRISM_historical_run1_197101-200012.nc',
                    'pr_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/pr_monClim_PRISM_historical_run1_197101-200012.nc',
                    'tmin_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/tmin_monClim_PRISM_historical_run1_197101-200012.nc'}


def test_raster_metadata_minmax(raster_metadata):
    req = Request.blank('?request=GetMinMax&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax')
    resp = req.get_response(raster_metadata)

    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'

    stats = json.loads(resp.body)
    assert len(stats) == 2
    assert set(stats.keys()) == set(['max', 'min'])


def test_raster_metadata_minmax_w_units(raster_metadata):
    req = Request.blank('?request=GetMinMaxWithUnits&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax')
    resp = req.get_response(raster_metadata)

    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'

    stats = json.loads(resp.body)
    assert len(stats) == 3
    assert set(stats.keys()) == set(['max', 'min', 'units'])


def test_raster_metadata_minmax_no_id(raster_metadata):
    req = Request.blank('?request=INVALID_REQUEST_TYPE&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231&var=tasmax')
    resp = req.get_response(raster_metadata)
    assert resp.status == '400 Bad Request'


def test_raster_metadata_minmax_no_id(raster_metadata):
    req = Request.blank('?request=GetMinMax&var=tasmax')
    resp = req.get_response(raster_metadata)
    assert resp.status == '400 Bad Request'


def test_raster_metadata_minmax_no_var(raster_metadata):
    req = Request.blank('?request=GetMinMax&id=pr-tasmax-tasmin_day_BCSD-ANUSPLIN300-CanESM2_historical-rcp26_r1i1p1_19500101-21001231')
    resp = req.get_response(raster_metadata)
    assert resp.status == '400 Bad Request'


def test_raster_metadata_minmax_bad_id(raster_metadata):
    req = Request.blank('?request=GetMinMax&id=NOT_A_VALID_ID&var=tasmax')
    resp = req.get_response(raster_metadata)
    assert resp.status == '404 Not Found'
