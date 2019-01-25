from genshi.template import TemplateLoader
from sqlalchemy import or_, not_

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
            'elements': self.get_elements(environ.get('sesh', None)),
            'version': str(pydap.lib.__version__)
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

    def get_elements(self, sesh):
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

    def get_elements(self, sesh):
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

    def get_elements(self, sesh):
        '''Runs a database query and returns a list of (``network_name``, ``network_description``) pairs for which there exists either climo or raw data.
        '''
        # Join to vars_per_history to make sure data exists for
        # stations in each network, but don't actually return anything
        # associated with that table
        query = sesh.query(Network.name, Network.long_name).join(Variable).\
                join(VarsPerHistory).distinct().order_by(Network.name)
        return query.all()

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

    def get_elements(self, sesh):

        '''Runs a database query and returns a list of (``native_id``,
        ``station_name``) pairs which are in the given PCDS network.
        '''
        network_name = self.args['network']

        # Join to vars_per_history to make sure data exists for each
        # station, but don't actually return anything associated with
        # that table
        if self.args['is_climo']:
            query = sesh.query(Station.native_id, History.station_name).join(History).join(Network).join(VarsPerHistory).join(Variable).\
                    filter(Network.name == network_name).\
                    filter(or_(Variable.cell_method.contains('within'), Variable.cell_method.contains('over'))).\
                    distinct().order_by(Station.native_id)
        else:
            query = sesh.query(Station.native_id, History.station_name).join(History).join(Network).join(VarsPerHistory).join(Variable).\
                    filter(Network.name == network_name).\
                    filter(not_(or_(Variable.cell_method.contains('within'), Variable.cell_method.contains('over')))).\
                    distinct().order_by(Station.native_id)

        return query.all()

