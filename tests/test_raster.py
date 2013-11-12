from pdp_util.raster import ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
import json

import pytest
from webob.request import Request

@pytest.fixture(scope="function")
def config():
    return {'ensemble': 'bc_prism', 'root_url': 'http://basalt.pcic.uvic.ca:8080/data/'}

def test_ensemble_files(mm_session):
    result = ensemble_files(mm_session, 'bc_prism')
    assert result == {'bcprism_ppt_7100': '/home/data/projects/PRISM/dataportal/bc_ppt_7100.nc',
                      'bcprism_tmax_7100': '/home/data/projects/PRISM/dataportal/bc_tmax_7100.nc',
                      'bcprism_tmin_7100': '/home/data/projects/PRISM/dataportal/bc_tmin_7100.nc'}


def test_db_raster_catalog(mm_session, config):
    result = db_raster_catalog(mm_session, config['ensemble'], config['root_url'])
    assert result == {'bcprism_ppt_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_ppt_7100.nc',
                      'bcprism_tmax_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmax_7100.nc',
                      'bcprism_tmin_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmin_7100.nc'}
    
def test_db_raster_configurator(mm_session):
    args = [mm_session, 'A name', 'Version 0.0.0.1', '0.0', 'bc_prism', 'http://basalt.pcic.uvic.ca:8080/data/']
    result = db_raster_configurator(*args)
    assert result == {'api_version': '0.0',
                      'ensemble': 'bc_prism',
                      'handlers': [{'file': '/home/data/projects/PRISM/dataportal/bc_ppt_7100.nc',
                                     'url': 'bc_ppt_7100.nc'},
                                   {'file': '/home/data/projects/PRISM/dataportal/bc_tmax_7100.nc',
                                    'url': 'bc_tmax_7100.nc'},
                                   {'file': '/home/data/projects/PRISM/dataportal/bc_tmin_7100.nc',
                                    'url': 'bc_tmin_7100.nc'}],
                      'name': 'A name',
                      'root_url': 'http://basalt.pcic.uvic.ca:8080/data/',
                      'version': 'Version 0.0.0.1'}

@pytest.mark.parametrize('url', ['', '/url/is/irrellevant'])
def test_ensemble_catalog(mm_dsn, config, url):
    app = EnsembleCatalog(mm_dsn, config)
    req = Request.blank(url)
    resp = req.get_response(app)
    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    body = json.loads(resp.body)
    assert body == {'bcprism_ppt_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_ppt_7100.nc',
                      'bcprism_tmax_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmax_7100.nc',
                      'bcprism_tmin_7100': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmin_7100.nc'}
