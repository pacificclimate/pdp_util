import re
import random
import cPickle
from tempfile import mkdtemp
from shutil import rmtree
from pkg_resources import resource_filename

import pytest
from webob.request import Request
from beaker.middleware import SessionMiddleware

from pdp_util.auth import PcicOidMiddleware

beaker_session_id = None

@pytest.fixture(scope="function")
def authorized_session(request):
    global beaker_session_id
    
    def fake_app(environ, start_response):
        start_response('200 OK', [])
        return ['Content']

    auth_dir = mkdtemp()
    oid_app = PcicOidMiddleware(fake_app, resource_filename('pdp_util', 'templates'), '')
    auth_app = SessionMiddleware(oid_app, data_dir=auth_dir, auto=True, log='/tmp/auth_log.txt')

    # FIXME: I shouldn't have to do this, but the store doesn't get initialized until the first request
    try:
        oid_app({}, None)
    except:
        pass

    assoc_handle = 'handle'
    saved_assoc = 'saved'
    claimed_id = 'test_id'
    oid_app.store.add_association(claimed_id, None, saved_assoc)
    oid_app.store.add_association(assoc_handle, None, saved_assoc)

    session = str(random.getrandbits(40))
    oid_app.store.start_login(session, cPickle.dumps((claimed_id, assoc_handle)))

    # Simulate the return from the openid provider
    req = Request.blank('/?openid_return='+session+'&openid.signed=yes')
    resp = req.get_response(auth_app)
    assert resp.status == '200 OK'

    m = re.search(r'beaker.session.id=([a-f0-9]+);', resp.headers['Set-cookie'])
    beaker_session_id = m.group(1)

    def fin():
        print ("finalizing %s and removing %s" % (authorized_session, auth_dir))
        rmtree(auth_dir, ignore_errors=True)

    request.addfinalizer(fin)
    return auth_app


@pytest.mark.skip(reason="Moving to client-side auth soon")
def test_redirect(authorized_session):
    req = Request.blank('/?openid_identifier=https://www.google.com/accounts/o8/id')
    resp = req.get_response(authorized_session)
    assert resp.status == '303 Go to OpenID provider'
    
def test_auth_blocking(authorized_session):
    # Ensure that it won't let us behind the auth wall
    url = ''
    req = Request.blank(url)
    resp = req.get_response(authorized_session)
    assert resp.status == '401 Unauthorized'

def test_auth_passthrough(authorized_session):
    global beaker_session_id

    url = ''
    req = Request.blank(url)
    req.cookies['beaker.session.id'] = beaker_session_id
    resp = req.get_response(authorized_session)
    assert resp.status == '200 OK'
