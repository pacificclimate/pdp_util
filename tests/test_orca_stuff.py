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
    'root_url': 'http://tools.pacificclimate.org/dataportal/data/vic_gen1/',
    'name': 'testing-server',
    'version': 0,
    'api_version': 0,
    'handlers': [
        {
            'url': 'tasmax_day_BCCAQv2_CanESM2_historical-rcp85_r1i1p1_19500101-21001231_Canada.nc',
            'file': 'tasmax_day_BCCAQv2_CanESM2_historical-rcp85_r1i1p1_19500101-21001231_Canada.nc'
        },
        {
            'url': 'pr+tasmin+tasmax+wind_day_CGCM3_A1B_run1_19500101-21001231.nc',
            'file': '/storage/data/projects/dataportal/data/vic_gen1_input/pr+tasmin+tasmax+wind_day_CGCM3_A1B_run1_19500101-21001231.nc'
        },
    ],
    'ensemble' : 'bccaq2',
    'thredds_root' : 'http://docker-dev03.pcic.uvic.ca:30333'
}


environ = {
    'PATH_INFO' : 'tasmax_day_BCCAQv2_CanESM2_historical-rcp85_r1i1p1_19500101-21001231_Canada.nc.nc',
    'QUERY_STRING' : 'tasmax[0:150][0:91][0:206]&',
}

def test_RasterServer(mm_database_dsn):

    r_server = RasterServer(mm_database_dsn, config)
    resp = r_server(environ, start_response)

    print(resp)
    assert resp.status_code == 301
    assert config['thredds_root'] in resp.location
