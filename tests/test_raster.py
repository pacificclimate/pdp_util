from pdp_util.raster import ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
import json

import pytest
from webob.request import Request

@pytest.fixture(scope="function")
def config():
    return {'ensemble': 'bc_prism', 'root_url': 'http://basalt.pcic.uvic.ca:8080/data/'}

def test_ensemble_files(mm_session):
    result = ensemble_files(mm_session, 'bc_prism')
    assert result == {'tmax_monClim_PRISM_historical_run1_197101-200012': '/home/data/climate/PRISM/dataportal/tmax_monClim_PRISM_historical_run1_197101-200012.nc',
                      'pr_monClim_PRISM_historical_run1_197101-200012': '/home/data/climate/PRISM/dataportal/pr_monClim_PRISM_historical_run1_197101-200012.nc',
                      'tmin_monClim_PRISM_historical_run1_197101-200012': '/home/data/climate/PRISM/dataportal/tmin_monClim_PRISM_historical_run1_197101-200012.nc'}


def test_db_raster_catalog(mm_session, config):
    result = db_raster_catalog(mm_session, config['ensemble'], config['root_url'])
    assert result == {'tmax_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/tmax_monClim_PRISM_historical_run1_197101-200012.nc',
                      'pr_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/pr_monClim_PRISM_historical_run1_197101-200012.nc',
                      'tmin_monClim_PRISM_historical_run1_197101-200012': 'http://basalt.pcic.uvic.ca:8080/data/tmin_monClim_PRISM_historical_run1_197101-200012.nc'}
    
def test_db_raster_configurator(mm_session):
    args = [mm_session, 'A name', 'Version 0.0.0.1', '0.0', 'bc_prism', 'http://basalt.pcic.uvic.ca:8080/data/']
    result = db_raster_configurator(*args)
    assert all([x in result.items() for x in {'root_url': 'http://basalt.pcic.uvic.ca:8080/data/',
                                             'name': 'A name',
                                             'version': 'Version 0.0.0.1',
                                             'ensemble': 'bc_prism',
                                             'api_version': '0.0'}.items()])

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
