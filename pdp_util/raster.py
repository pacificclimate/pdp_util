import os
from os.path import basename

from pydap.wsgi.app import DapServer
from pdp_util import session_scope
from modelmeta import DataFile, DataFileVariable, EnsembleDataFileVariables, Ensemble

from simplejson import dumps
from webob.request import Request
from webob.response import Response

pwd = os.getcwd()
config = {'name': 'testing-server',
          'version': 0,
          'api_version': 0,
          'handlers': [{'url': '/my.nc',
                        'file': os.path.join(pwd, 'miroc_3.2_20c_A1B_daily_nc3_0_100.nc')},
                       {'url': '/my.h5',
                        'file': os.path.join(pwd, 'pr+tasmax+tasmin_day_BCCA+ANUSPLIN300+CanESM2_historical+rcp26_r1i1p1_19500101-21001231.h5')},
                       {'url': '/stuff/',
                        'dir': '/home/data/climate/downscale/CMIP5/anusplin_downscaling_cmip5/downscaling_outputs/'},
                       ]
          }


class RasterServer(DapServer):
    '''WSGI app which is a subclass of PyDap's DapServer to do dynamic (non-filebased) configuration, for serving rasters'''
    def __init__(self, dsn, config=config):
        '''Initialize the application

           :param config: A config dict that can be read by :py:func:`yaml.load` and includes the key `handlers`. `handlers` must be a list of dicts each containing the keys: `url` and `file`.
           :type config: dict
        '''
        DapServer.__init__(self, None)
        self._config = config
        self.dsn = dsn

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):
        '''An override of Pydap's __call__ which overrides catalog requests, but defers to pydap for data requests'''
        req = Request(environ)

        if req.path_info == '/catalog.json':
            with session_scope(self.dsn) as sesh:
                urls = db_raster_catalog(sesh, self.config['ensemble'], self.config['root_url'])
            res = Response(
                    body=dumps(urls, indent=4),
                    content_type='application/json',
                    charset='utf-8')
            return res(environ, start_response)
        else:
            return super(RasterServer, self).__call__(environ, start_response)

class RasterCatalog(RasterServer):
    '''WSGI app which is a subclass of RasterServer.  Filters the urls on call to permit only MetaData requests'''

    def __call__(self, environ, start_response):
        '''An override of RasterServer's __call__ which allows only MetaData requests'''
        req = Request(environ)
        if req.path_info in ['/', '/catalog.json']:
            environ['PATH_INFO'] = '/catalog.json'
            return super(RasterCatalog, self).__call__(environ, start_response)
        elif req.path_info.split('.')[-1] in ['das', 'dds']:
            return super(RasterCatalog, self).__call__(environ, start_response)
        else:
            start_response('404 Not Found', [])
            return [str(req.path_info), ' not found']

class EnsembleCatalog(object):
    '''WSGI app to list an ensemble catalog'''
    def __init__(self, dsn, config=config):
        self._config = config
        self.dsn = dsn

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):
        with session_scope(self.dsn) as sesh:
            urls = db_raster_catalog(sesh, self.config['ensemble'], self.config['root_url'])
        res = Response(
            body=dumps(urls, indent=4),
            content_type='application/json',
            charset='utf-8')
        return res(environ, start_response)

def db_raster_catalog(session, ensemble, root_url):
    '''A function which queries the database for all of the raster files belonging to a given ensemble. Returns a dict where keys are the dataset unique ids and the value is the filename for the dataset.

       :param session: SQLAlchemy session for the pcic_meta database
       :param ensemble: Name of the ensemble for which member files should be listed
       :param root_url: Base URL which should be prepended to the beginning of each dataset ID
       :rtype: dict
    '''
    files = ensemble_files(session, ensemble)
    return { id.replace('+', '-'): root_url + basename(filename) for id, filename in files.items() } #FIXME: remove the replace when ncwms stops being dumb

def db_raster_configurator(session, name, version, api_version, ensemble, root_url='/'):
    '''A function to construct a config dict which is usable for configuring Pydap for serving rasters

       :param session: SQLAlchemy session for the pcic_meta database
       :param name: Name of this server e.g. `my-raster-server`
       :param version: Version of the server application
       :param api_version: OPeNDAP API version?
       :param ensemble: The identifier for the PCIC MetaData DataBase (:class:`Mddb`) ensemble to configure
       :param root_url: URL to prepend to all of the dataset ids
    '''
    files = ensemble_files(session, ensemble)
    config = {'name': name,
              'version': version,
              'api_version': api_version,
              'ensemble': ensemble,
              'root_url': root_url,
              'handlers': [{'url': basename(filename), 'file': filename} for id, filename in files.items()]
              }
    return config

def ensemble_files(session, ensemble_name):
    q = session.query(DataFile).join(DataFileVariable).join(EnsembleDataFileVariables).join(Ensemble).filter(Ensemble.name == ensemble_name)
    return { row.unique_id: row.filename for row in q }
