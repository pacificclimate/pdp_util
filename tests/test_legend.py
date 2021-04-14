from datetime import datetime, timedelta

import pytz
from webob.request import Request

from pdp_util.legend import LegendApp


def test_can_initialize(conn_params):
    app = LegendApp(conn_params)
    assert True


def test_legend_app(conn_params):
    app = LegendApp(conn_params)
    req = Request.blank("/apps/legend/flnro-wmb.png")
    resp = req.get_response(app)
    assert resp.status == "200 OK"
    assert resp.content_type == "application/png"
    # FIXME: should actually find a way to test the image color... load it with PIL?


def test_caching(conn_params):
    url = "/apps/legend/flnro-wmb.png"

    app = LegendApp(conn_params)
    server_load_time = app.load_time
    pre_load_time = server_load_time - timedelta(0, 60)
    post_load_time = server_load_time + timedelta(0, 60)

    req = Request.blank(url)
    resp = req.get_response(app)

    # Test that it properly returns a NotModified
    req = Request.blank(url)
    req.if_modified_since = post_load_time
    resp = req.get_response(app)
    assert resp.status.startswith("304")

    # Test that it properly returns updated content if necessary
    req = Request.blank(url)
    req.if_modified_since = pre_load_time
    resp = req.get_response(app)
    assert resp.status.startswith("200")


def test_unknown_network(conn_params):
    app = LegendApp(conn_params)
    req = Request.blank("/apps/legend/unknown_network.png")
    resp = req.get_response(app)
    assert resp.status == "200 OK"
    assert resp.content_type == "application/png"
    # FIXME: should actually find a way to test the image color... load it with PIL?


def test_404s(conn_params):
    app = LegendApp(conn_params)
    urls = ["", "missing_leading_slash.png", "/a/directory/", "doesnt_end_with_png.txt"]

    for url in urls:
        req = Request.blank(url)
        resp = req.get_response(app)
        assert resp.status.startswith("404")
