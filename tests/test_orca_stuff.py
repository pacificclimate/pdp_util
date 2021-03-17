from pdp_util.raster import RasterServer
import pytest
import os
import sys


def write(data):
    if not headers_set:
         raise AssertionError("write() before start_response()")

    elif not headers_sent:
         # Before the first output, send the stored headers
         status, response_headers = headers_sent[:] = headers_set
         sys.stdout.write('Status: %s\r\n' % status)
         for header in response_headers:
             sys.stdout.write('%s: %s\r\n' % header)
         sys.stdout.write('\r\n')

    sys.stdout.write(data)
    sys.stdout.flush()

headers_set = []
headers_sent = []

def start_response(status, response_headers, exc_info=None):
    if exc_info:
        try:
            if headers_sent:
                # Re-raise original exception if headers sent
                raise exc_info[0], exc_info[1], exc_info[2]
        finally:
            exc_info = None     # avoid dangling circular ref
    elif headers_set:
        raise AssertionError("Headers already set!")

    headers_set[:] = [status, response_headers]
    return write


pwd = os.getcwd()
config = {
    'name': 'testing-server',
    'version': 0,
    'api_version': 0,
    'handlers': [
        {
            'url': '/my.nc',
            'file': os.path.join(pwd, 'miroc_3.2_20c_A1B_daily_nc3_0_100.nc')
        },
        {
            'url': '/my.h5',
            'file': os.path.join(pwd, 'pr+tasmax+tasmin_day_BCCA+ANUSPLIN300+CanESM2_historical+rcp26_r1i1p1_19500101-21001231.h5')
        },
        {
            'url': '/stuff/',
            'dir': '/home/data/climate/downscale/CMIP5/anusplin_downscaling_cmip5/downscaling_outputs/'
        },
    ],
    'ensemble' : 'bccaq2',
    'root_url' : 'root'
}

environ = dict(os.environ.items())

def setup_environ():
    environ['wsgi.input'] = sys.stdin
    environ['wsgi.errors'] = sys.stderr
    environ['wsgi.version'] = (1, 0)
    environ['wsgi.multithread'] = False
    environ['wsgi.multiprocess'] = False
    environ['wsgi.run_once'] = True
    environ['wsgi.url_scheme'] = 'http'
    environ['PATH_INFO'] = '/data' # what we have in query string may actually belong appended to here
    environ['QUERY_STRING'] = 'tasmax_day_BCCAQv2_CanESM2_historical-rcp85_r1i1p1_19500101-21001231_Canada/tasmax[0:150][0:91][0:206]'
    environ['REQUEST_METHOD'] = 'GET'

def test_RasterServer(mm_database_dsn):
    setup_environ()

    r_server = RasterServer(mm_database_dsn, config)
    data = r_server(environ, start_response)

    print(data)
    assert data != None
    assert False
