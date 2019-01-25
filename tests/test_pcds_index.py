from os.path import dirname
from pkg_resources import resource_filename

from webob.request import Request
from bs4 import BeautifulSoup


from pdp_util.pcds_index import PcdsIsClimoIndex, PcdsNetworkIndex, PcdsStationIndex
from pycds import *

def make_common_assertions(resp):
    assert resp.status == '200 OK'
    assert resp.content_type == 'text/html'
    assert resp.content_length < 0
    print resp.body

def test_climo_index(conn_params):
    app = PcdsIsClimoIndex(app_root='/', templates=resource_filename('pdp_util', 'templates'), conn_params=conn_params) #FIXME: template path is fragile

    assert app.get_elements() == (('climo', 'Climatological calculations'),
                                  ('raw', 'Raw measurements from participating networks'))

    req = Request.blank('')
    resp = req.get_response(app)
    make_common_assertions(resp)

    assert "Climatological calculations" in resp.body
    assert "raw/" in resp.body
    
def test_network_index(conn_params):
    app = PcdsNetworkIndex(app_root='/', templates=resource_filename('pdp_util', 'templates'), conn_params=conn_params, is_climo=False) #FIXME: template path is fragile

    assert app.get_elements() == [('AGRI', 'BC Ministry of Agriculture'),
                                  ('ARDA', 'Agricultural and Rural Development Act Network'),
                                  ('EC', 'Environment Canada (Canadian Daily Climate Data 2007)'),
                                  ('EC_raw', 'Environment Canada (raw observations from "Climate Data Online")'),
                                  ('ENV-ASP', 'BC Ministry of Environment - Automated Snow Pillow Network'),
                                  ('FLNRO-WMB', 'BC Ministry of Forests, Lands, and Natural Resource Operations - Wild Fire Managment Branch'),
                                  ('MoTIe', 'Ministry of Transportation and Infrastructure (electronic)')]

    req = Request.blank('/pcds/raw/')
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.body, features="html.parser")
    
    assert "Participating CRMP Networks" in soup.title.string
    assert "FLNRO-WMB/" in resp.body
    assert "Environment Canada (Canadian Daily Climate Data 2007)" in resp.body

def test_station_index(conn_params):

    app = PcdsStationIndex(app_root='/', templates=resource_filename('pdp_util', 'templates'), conn_params=conn_params, is_climo=False, network='AGRI') #FIXME: template path is fragile

    assert app.get_elements() == [('de107', 'Deep Creek')]

    req = Request.blank('/pcds/raw/AGRI/')
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.body, features="html.parser")
    
    assert "Stations for network AGRI" in soup.title.string
    assert "de107/" in resp.body
    assert "Deep Creek" in resp.body

    assert "de107climo/" not in resp.body
    assert 'Deep "Climo Station" Creek' not in resp.body

def test_station_index_for_climatologies(conn_params, test_session):

    app = PcdsStationIndex(app_root='/', templates=resource_filename('pdp_util', 'templates'), conn_params=conn_params, is_climo=True, network='AGRI')

    assert app.get_elements() == [('de107climo', 'Deep "Climo Station" Creek')]

    req = Request.blank('/pcds/climo/AGRI/')
    resp = req.get_response(app)
    make_common_assertions(resp)

    soup = BeautifulSoup(resp.body, features="html.parser")

    assert "Stations for network AGRI" in soup.title.string
    assert "de107/" not in resp.body
    assert "Deep Creek" not in resp.body
    assert "de107climo/" in resp.body
    assert 'Deep "Climo Station" Creek' in resp.body
