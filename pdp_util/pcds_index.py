import re

from sqlalchemy.sql.expression import select
from genshi.template import TemplateLoader

import pydap.lib
from pdp_util import session_scope
from pycds import Network, Variable, VarsPerHistory, Station, History

class PcdsIndex(object):
    '''WSGI app which is a base class for templating database dependent listings
    
       The app should be configured with local args ``conn_params`` so that it can make a database connection

       Subclasses must implement the :func:`get_elements` method which returns an iterable of 2-tuples (the things to list)

       Subclasses may set the options in ``kwargs``: ``title``, ``short_name``, ``long_name``

       :param conn_params:
       :type conn_params: dict
       :param app_root: The absolute URL of where this application lives
       :type app_root: str
       :param templates: filesystem path to where the templates live
       :type templates: str
    '''
    def __init__(self, **kwargs):
        self.args = kwargs
        def session_scope_factory():
            return session_scope(kwargs['conn_params'])
        self.session_scope_factory = session_scope_factory


    def __call__(self, environ, start_response):
        self.args['root'] = self.args['app_root'] # WTF?
        environ['pcic.app_root'] = self.args['app_root']
        status = '200 OK'
        response_headers = [('Content-type', 'text/html')]
        start_response(status, response_headers)
        
        params = self.args
        params.update({
            'environ': environ,
            'elements': self.get_elements(),
            'version': u'.'.join(str(d) for d in pydap.lib.__version__)
            })
        return self.render(**params)

    def render(self, **kwargs):
        '''Loads and renders the index page template and returns an HTML stream

           :param elements: a list of (``name``, ``description``) pairs which will be listed on the index page
           :type elements: list

           :rtype: str
        '''
        loader = TemplateLoader(self.args['templates'])
        tmpl = loader.load('pcds_index.html')
        stream = tmpl.generate(**kwargs)
        return stream.render('html', doctype='html', encoding='UTF-8')

    def get_elements(self):
        '''Stub function

           :raises: NotImplementedError
        '''
        raise NotImplementedError

class PcdsIsClimoIndex(PcdsIndex):
    '''WSGI app which renders an index page just showing "climo" and "raw". Super simple.
    '''
    def __init__(self, **kwargs):
        '''
           :param title: Title for the index page
           :type title: str
           :param short_name: First column header (usually a short name)
           :type short_name: str:
           :param long_name: Second column header (usually a longer description)
           :type long_name: str
        '''
        defaults = {'title': u'PCDS Data',
                    'short_name': u'Data type',
                    'long_name': u'Data type decription'}
        kwargs = dict(list(defaults.items()) + list(kwargs.items()))
        PcdsIndex.__init__(self, **kwargs)

    def get_elements(self):
        return (('climo', 'Climatological calculations'), ('raw', 'Raw measurements from participating networks'))
    
class PcdsNetworkIndex(PcdsIndex):
    '''WSGI app which renders an index page for all of the networks in the PCDS
    '''
    def __init__(self, **kwargs):
        '''
           :param title: Title for the index page
           :type title: str
           :param short_name: First column header (usually a short name)
           :type short_name: str:
           :param long_name: Second column header (usually a longer description)
           :type long_name: str
           :param is_climo: Is this an index for climatolies rather than raw data?
           :type is_climo: Boolean
        '''
        defaults = {'title': 'Participating CRMP Networks',
                    'short_name': 'Network name',
                    'long_name': 'Network description'}
        kwargs = dict(list(defaults.items()) + list(kwargs.items())) # FIXME: defaults.update()?
        PcdsIndex.__init__(self, **kwargs)

    def get_elements(self):
        '''Runs a database query and returns a list of (``network_name``, ``network_description``) pairs for which there exists either climo or raw data.
        '''
        with self.session_scope_factory() as sesh:
            query = sesh.query(Network.name, Network.long_name, Variable.cell_method).join(Variable).join(VarsPerHistory).distinct().order_by(Network.name)

            # _could_ do this in a where clause, but there seem to be no good cross database regex queries (runs on Postgres, but not sqlite)
            # FIXME: this is super slow. Probably too much data transfer
            pattern = '(within|over)'
            elements = [ (net_name, net_long_name) for net_name, net_long_name, cell_method in query.all() if ~(self.args['is_climo'] ^ bool(re.search(pattern, cell_method))) ]
        elements = list(set(elements))
        elements.sort()
        return elements

class PcdsStationIndex(PcdsIndex):
    '''WSGI app which renders an index page for all of the stations in a given PCDS network
    '''
    def __init__(self, **kwargs):
        '''
           :param title: Title for the index page
           :type title: str
           :param short_name: First column header (usually a short name)
           :type short_name: str:
           :param long_name: Second column header (usually a longer description)
           :type long_name: str
        '''
        defaults = {'title': 'Stations for network %(network)s' % kwargs,
                    'short_name': "Network Station ID",
                    'long_name': "Station name"}
        kwargs = dict(list(defaults.items()) + list(kwargs.items())) # FIXME: defaults.update()?
        PcdsIndex.__init__(self, **kwargs)

    def get_elements(self):
        '''Runs a database query and returns a list of (``native_id``, ``station_name``) pairs which are in the given PCDS network.
        '''
        network_name = self.args['network']
        with self.session_scope_factory() as sesh:
            query = sesh.query(Station.native_id, History.station_name, Variable.cell_method).join(History).join(Network).join(Variable).join(VarsPerHistory)\
              .filter(Network.name == network_name)\
              .distinct().order_by(Station.native_id)

            pattern = '(within|over)'
            # FIXME: this is super slow. Probably too much data transfer
            elements = [ (native_id, station_name) for native_id, station_name, cell_method in query.all() \
                         if ~(self.args['is_climo'] ^ bool(re.search(pattern, cell_method)))]
        elements = list(set(elements))
        elements.sort()
        return elements

