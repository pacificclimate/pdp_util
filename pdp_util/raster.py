import os
from os.path import basename

from pydap.wsgi.app import DapServer
from pdp_util.mddb import Mddb

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
    def __init__(self, config=config):
        '''Initialize the application

           :param config: A config dict that can be read by :py:func:`yaml.load` and includes the key `handlers`. `handlers` must be a list of dicts each containing the keys: `url` and `file`.
           :type config: dict
        '''
        DapServer.__init__(self, None)
        self._config = config

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):
        '''An override of Pydap's __call__ which overrides catalog requests, but defers to pydap for data requests'''
        req = Request(environ)

        if req.path_info == '/catalog.json':
            urls = self.ourcatalog(req)
            res = Response(
                    body=dumps(urls, indent=4),
                    content_type='application/json',
                    charset='utf-8')
            return res(environ, start_response)
        else:
            return super(RasterServer, self).__call__(environ, start_response)


    def ourcatalog(self, req):
        '''
        Return a JSON listing of the datasets served.

        :param req: FIXME: Unused?
        '''
        return db_raster_catalog(self.config)

class EnsembleCatalog(object):
    '''WSGI app to list an ensemble catalog'''
    def __init__(self, config=config):
        self._config = config

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):

        urls = db_raster_catalog(self.config)
        res = Response(
            body=dumps(urls, indent=4),
            content_type='application/json',
            charset='utf-8')
        return res(environ, start_response)


def db_raster_catalog(config):
    '''A function which queries the database for all of the raster files belonging to a given ensemble. Returns a dict where keys are the dataset unique ids and the value is the filename for the dataset.
    
       :param config: A config dictionary which contains keys: `ensemble` and `root_url`
       :type config: dict
       :rtype: dict
    '''
    m = Mddb(config['ensemble'])
    return { id.replace('+', '-'): config['root_url'] + basename(filename) for id, filename in m.files.items() } #FIXME: remove the replace when ncwms stops being dumb

def db_raster_configurator(name, version, api_version, ensemble, root_url='/'):
    '''A function to construct a config dict which is usable for configuring Pydap for serving rasters

       :param name: Name of this server e.g. `my-raster-server`
       :param version: Version of the server application
       :param api_version: OPeNDAP API version?
       :param ensemble: The identifier for the PCIC MetaData DataBase (:class:`Mddb`) ensemble to configure
       :param root_url: URL to prepend to all of the dataset ids
    '''
    m = Mddb(ensemble)
    files = m.files
    config = {'name': name,
              'version': version,
              'api_version': api_version,
              'ensemble': ensemble,
              'root_url': root_url,
              'handlers': [{'url': basename(filename), 'file': filename} for id, filename in files.items()]
              }
    return config
