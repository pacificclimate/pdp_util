from webob.request import Request
from simplejson import loads

def test_ensemble_member_lister(ensemble_member_lister):
    req = Request.blank('?ensemble_name=canada_map')
    resp = req.get_response(ensemble_member_lister)

    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    
    ensembles = loads(resp.body)
    # Good enough
    assert len(ensembles) == 3
    assert set(ensembles.keys()) == set(['historical+rcp26', 'historical+rcp45', 'historical+rcp85'])

def test_ensemble_bad_request(ensemble_member_lister):
    req = Request.blank('')
    resp = req.get_response(ensemble_member_lister)
    assert resp.status == '400 Bad Request'


def test_ensemble_does_not_exist(ensemble_member_lister):
    req = Request.blank('?ensemble_name=non_existant')
    resp = req.get_response(ensemble_member_lister)

    assert resp.status == '200 OK'
    assert resp.body
