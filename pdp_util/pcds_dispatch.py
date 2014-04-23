import logging

from pydap.handlers.pcic import RawPcicSqlHandler, ClimoPcicSqlHandler
from pdp_util.pcds_index import PcdsNetworkIndex, PcdsStationIndex, PcdsIsClimoIndex
from paste.httpexceptions import HTTPNotFound, HTTPSeeOther

logger = logging.getLogger(__name__)

class PcdsDispatcher(object):
    '''This class is a WSGI app which interprets parts of a URL and routes the request to one of several handlers
    
       It is assumed that the URL points to something like http://tools.pacificclimate.org/data_portal/pydap/pcds/raw/MoE/0260011/
       
       In this case ``PATH_INFO`` will be /raw/MoE/0260011/

       The dispatcher breaks the url pieces into three parts:
              
       1. ``is_climo`` = (raw|climo) i.e. should the app be looking for climatologies or raw observations
       2. ``network``: the short network abbreviation
       3. ``station``: this is the native_id in the database

       If ``is_climo`` is unspecified, the app will route to :py:class:`pcic.pcds_index.PcdsIsClimoIndex`
       
       If ``is_climo`` is incorrectly specified, the app will return a 404 :py:class:`HTTPNotFound`

       If ``network`` is unspecified, the app will route to :py:class:`pcic.pcds_index.PcdsNetworkIndex`

       If ``network`` is specified as a non-existent network, it will just show an empty network listing

       If ``station`` is unspecified, the app will route to :py:class:`pcic.pcds_index.PcdsStationIndex`

       If ``station`` is specified as a non-existant station, it will return a 404 :py:class:`HTTPNotFound`

       If any extra garbage is found on the end of an otherwise valid path, the app will redirect with
       an :py:class:`HTTPSeeOther` to the :py:class:`pcic.pcds_index.PcdsStationIndex` for the specified station
    '''
    def __init__(self, **kwargs):
        '''Initialize the app. Generally these arguments will all come out of the global config.

        :param templates:
        :param app_root:
        :param ol_path:
        :param conn_params:
        '''
        self.kwargs = kwargs

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']
        environ.update({'pcic.app_root': self.kwargs['app_root'], 'pcic.ol_path': self.kwargs['ol_path']})
        responder_class, responder_args, responder_kwargs, new_env = self._route_request(path)
        responder = responder_class(*responder_args, **responder_kwargs)
        environ.update(new_env)
        return responder(environ, start_response)

    def _route_request(self, path):
        '''Returns a tuple of (responder_class, responder_args, responder_kwargs, new_request_environment)

        :param path: the PATH_INFO of the request
        '''
        logger.debug("Attempting to route a request to path: {}".format(path))
        index_kwargs = self.kwargs
        if 'pcds' in path:
            path = path.split('pcds', 1)[1]
        url_parts = path.strip('/').split('/')
        logger.debug("url_parts", url_parts)

        try:
            is_climo = url_parts.pop(0)
            is_climo = {'climo': True, 'raw': False, '': None}[is_climo]
            if is_climo == None:
                index_kwargs['is_climo'] = None
                return PcdsIsClimoIndex, [], index_kwargs, {}

        except KeyError:
            return HTTPNotFound, ["First path element after pcds/ should be 'raw' or 'climo'"], {}, {} # 404

        try:
            index_kwargs.update({'is_climo': is_climo,
                                 'network': url_parts.pop(0),
                                 'index_class': 'station'})
            IndexClass = PcdsStationIndex
        except IndexError:
            index_kwargs.update({'is_climo': is_climo,
                                 'index_class': 'network'
                                 })
            IndexClass = PcdsNetworkIndex

        try:
            station = url_parts.pop(0) # If we can do this, it's at least a station listing

            # Station URL is too long... redirect back to the station
            if len(url_parts) > 0:
                if len(url_parts) == 1 and not path.endswith('/'):
                    new_path = "."
                else:
                    new_path = "../" * (len(url_parts) - (0 if path.endswith('/') else 1))
                return HTTPSeeOther, [new_path], {}, {}

            if is_climo:
                ext = 'csql'
                cls_ = ClimoPcicSqlHandler
            else:
                ext = 'rsql'
                cls_ = RawPcicSqlHandler

            env = {}
            if '.' not in path: # Assume that it's just a listing and not point to a dataset
                env['PATH_INFO'] = path.rstrip('/') + '.%s.html' % ext #FIXME html response!

            return cls_, [self.kwargs['conn_params']], {}, env

        except IndexError:
            logger.debug('Listing stations for a network')
            self.kwargs.update(index_kwargs)
            return IndexClass, [], self.kwargs, {}

        # This should never happen
        assert False
