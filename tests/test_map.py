from os.path import dirname, sep
from pkg_resources import resource_filename

import pytest
from bs4 import BeautifulSoup

from pdp_util.map import MapApp

kwargs = {'app_root': '',
          'templates': resource_filename('pdp_util', 'templates'),
          'ol_path': '',
          'proj_path': '',
          'geoserver_url': '',
          'ncwms_url': '',
          'version': '2.0',
          'title': 'My test MapApp',
          'map_div': '',
          'ensemble_name': '',
          'buttons': '',
          'header_block': '',
          'footer': ''
    }

def test_can_instantiate():
    map_app = MapApp(**kwargs)
    assert isinstance(map_app, MapApp)

def test_minimal_render():
    map_app = MapApp(**kwargs)
    text = map_app({}, lambda x, y: '')

    assert kwargs['title'] in text
    assert kwargs['version'] in text
    soup = BeautifulSoup(text)
    assert kwargs['title'] == soup.head.title.text
    
def test_missing_required_args():
    local_kwargs = kwargs.copy()
    del local_kwargs['geoserver_url']
    with pytest.raises(ValueError):
        map_app = MapApp(**local_kwargs)
