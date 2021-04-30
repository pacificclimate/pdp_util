from webob.request import Request
from simplejson import loads


def test_ensemble_member_lister(ensemble_member_lister):
    req = Request.blank("?ensemble_name=bcsd_downscale_canada")
    resp = req.get_response(ensemble_member_lister)

    assert resp.status == "200 OK"
    assert resp.content_type == "application/json"

    ensembles = loads(resp.app_iter)
    # Good enough
    assert len(ensembles) == 3
    assert set(ensembles.keys()) == set(
        ["historical+rcp85", "historical+rcp45", "historical+rcp26"]
    )


def test_ensemble_bad_request(ensemble_member_lister):
    req = Request.blank("")
    resp = req.get_response(ensemble_member_lister)
    assert resp.status == "400 Bad Request"


def test_ensemble_does_not_exist(ensemble_member_lister):
    req = Request.blank("?ensemble_name=non_existant")
    resp = req.get_response(ensemble_member_lister)

    assert resp.status == "404 Not Found"
    assert (
        resp.app_iter
        == '{"code": 404, "message": "Not Found", "details": "Ensemble named \'non_existant\' not found"}'
    )
