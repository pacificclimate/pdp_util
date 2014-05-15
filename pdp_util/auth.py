'''
The pdp_util.auth module contains apps and functions to handle the OpenID authentication to the data portal
'''

import json
import urllib
from collections import namedtuple

from openid2rp import get_email
from openid2rp.wsgi import Openid2Middleware
from openid2rp.wsgi.memstore import InMemoryStore
from webob.request import Request
from genshi.template import TemplateLoader

# supported providers
Provider = namedtuple('Provider', ['name', 'icon', 'url', 'signup'])
providers = [
    Provider('Launchpad', 'https://login.launchpad.net/favicon.ico', 'https://login.launchpad.net/', 'https://login.launchpad.net/pBkz56vSM5432lMr/+new_account'),
    Provider('myOpenID', 'https://www.myopenid.com/favicon.ico', 'https://www.myopenid.com/', 'https://www.myopenid.com/signup'),
    Provider('Verisign', 'http://pip.verisignlabs.com/favicon.ico', 'http://pip.verisignlabs.com', 'https://pip.verisignlabs.com/register.do'),
    Provider('Google', 'http://www.google.com/favicon.ico', 'https://www.google.com/accounts/o8/id', 'https://accounts.google.com/NewAccount'),
    Provider('Yahoo', 'http://www.yahoo.com/favicon.ico', 'http://yahoo.com/', 'https://edit.yahoo.com/registration?.src=fpctx&.intl=ca&.done=http%3A%2F%2Fca.yahoo.com%2F'),
    ]
    
class PcicOidMiddleware(Openid2Middleware):
    '''
    A WSGI filter which handles OpenID authentication process
    '''
    def __init__(self, app, templates, root, auth_required=True, return_to=None):
        '''
        Initialize the authorization middleware
        
        :param app: WSGI application to be wrapped
        :param templates: filesystem path to where the templates live
        :param root:
        :param auth_required: if True, lack of authorization will return a 401 if False, the login is only for optional identification
        :type auth_required: bool
        :param return_to: FIXME: this appears to be unused
        :type return_to: str or None        
        '''
        self.app = app
        self.store = None
        self.templates = templates
        self.root = root
        self.auth_required = auth_required

    # mostly ripped out of openid2rp.wsgi.demo
    def __call__(self, environ, start_response):
        self.store = SessionStore(environ)
        request = Request(environ)
        our_session = self.store.session

        self.return_to = urllib.quote(request.params.get('return_to', request.url))

        def login_401():
            '''Display a login page'''
            start_response('401 Unauthorized', [('Content-type','text/html; charset=utf-8')])
            notice = environ.get('openid2rp.notice', '') or ''
            params = {'notice': notice,
                      'providers': providers,
                      'root': self.root,
                      'return_to': self.return_to
                      }
            output = login_html(self.templates, **params)
            return [ output ]

        if 'openid_login' in request.params:
            return login_401()

        if 'openid_logout' in request.params:
            our_session.delete()
            if self.auth_required:
                return login_401()
            else:
                return self.app(environ, start_response)

        if 'openid_identifier' in request.params:
            oidid = request.params.get('openid_identifier')
            self.return_to = request.params.get('return_to') or request.referrer # This is _critical_ for getting returning to the users place in the app
            login_result_app = self.login(request, start_response) # This will modify the environ with 'openid2rp' stuff
            if 'openid2rp.notice' in environ and self.auth_required:
                return login_401()
            else:
                return login_result_app

        if 'openid_return' in request.params:
            our_session['email'] = get_email(request.url)
            our_session['openid_return'] = request.params['openid_return']
            return self.returned(request, start_response)

        # Check cookie
        elif 'openid_return' in our_session:
            environ['openid_return'] = our_session['openid_return']
            return self.returned(request, start_response)

        if 'openid2rp.error' in environ:
            start_response('401 Permission Denied', [('Content-type','text/plain')])
            return ['Something went wrong: '+environ['openid2rp.error']]

        if 'openid2rp.identifier' not in environ and self.auth_required:
            return login_401()

        # FIXME: Do we never check the OpenID association expiry?

        # Display authentication results
        return self.app(environ, start_response)

def login_html(tmpl_dir, **kwargs):
    '''Dispatches to the template renderer to generate the HTML of the login page
    '''
    loader = TemplateLoader(tmpl_dir)
    tmpl = loader.load('oid_login.html')
    stream = tmpl.generate(**kwargs)
    return stream.render('html', doctype='html', encoding='utf-8')

def check_authorized_return_email(environ, start_response):
    '''Simple WSGI application to test for whether the client is authenticated or not
       On success, returns a JSON response with the client's e-mail address. If not authorized, returns a 401 Unauthorized response.

    '''
    if 'beaker.session' in environ and 'email' in environ.get('beaker.session'):
        # The type _should_ be application/json, but IE dumbly treats it like a file download
        start_response('200 OK', [('Content-type', 'text/html; charset=utf-8')])
        return json.dumps({'email': environ.get('beaker.session')['email']})
    else:
        start_response('401 Unauthorized', [('Content-type', 'text/html; charset=utf-8')])
        return ['You are not logged in']

class SessionStore(InMemoryStore):
    '''Utility class used by PcicOidMiddleware to store the OpenID attributes
    '''
    def __init__(self, env):
        self.session = env.get('beaker.session', {})

        # ongoing logins
        # received reply nonces
        # established provider associations
        # heap of (time, dictionary, key) triples
        defaults = {'logins': {}, 'reply_nonces': {}, 'associations': {}, 'expirations': []}

        for key, val in defaults.items():
            if key not in self.session:
                self.session[key] = val
            setattr(self, key, self.session[key])
