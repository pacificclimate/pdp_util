from pdp_util.raster import ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
import json

import pytest
from webob.request import Request

@pytest.fixture(scope="function")
def config():
    return {'ensemble': 'bc_prism_demo', 'root_url': 'http://basalt.pcic.uvic.ca:8080/data/'}

def test_ensemble_files(mm_session):
    result = ensemble_files(mm_session, 'bc_prism_demo')
    assert result == {'bcprism_tmin_review_01': '/home/data/projects/PRISM/bc_tmin_review_01.nc', 'bcprism_ppt_review_14': '/home/data/projects/PRISM/bc_ppt_review_14.nc', 'bcprism_tmax_review_07': '/home/data/projects/PRISM/bc_tmax_review_07.nc'}


def test_db_raster_catalog(mm_session, config):
    result = db_raster_catalog(mm_session, config['ensemble'], config['root_url'])
    assert result == {'bcprism_ppt_review_14': 'http://basalt.pcic.uvic.ca:8080/data/bc_ppt_review_14.nc',
                      'bcprism_tmax_review_07': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmax_review_07.nc',
                      'bcprism_tmin_review_01': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmin_review_01.nc'}
    
def test_db_raster_configurator(mm_session):
    args = [mm_session, 'A name', 'Version 0.0.0.1', '0.0', 'bc_prism_demo', 'http://basalt.pcic.uvic.ca:8080/data/']
    result = db_raster_configurator(*args)
    assert result == {'api_version': '0.0',
                      'ensemble': 'bc_prism_demo',
                      'handlers': [{'file': u'/home/data/projects/PRISM/bc_tmin_review_01.nc',
                                    'url': u'bc_tmin_review_01.nc'},
                                    {'file': u'/home/data/projects/PRISM/bc_ppt_review_14.nc',
                                     'url': u'bc_ppt_review_14.nc'},
                                     {'file': u'/home/data/projects/PRISM/bc_tmax_review_07.nc',
                                      'url': u'bc_tmax_review_07.nc'}],
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
    assert body == {'bcprism_ppt_review_14': 'http://basalt.pcic.uvic.ca:8080/data/bc_ppt_review_14.nc',
                      'bcprism_tmax_review_07': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmax_review_07.nc',
                      'bcprism_tmin_review_01': 'http://basalt.pcic.uvic.ca:8080/data/bc_tmin_review_01.nc'}
