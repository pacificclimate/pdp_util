'''
This module provides aggregation utilities to translate a single HTTP request into multiple OPeNDAP requests, returning a single response
'''

from itertools import chain, imap, tee
from zipfile import ZipFile, ZIP_DEFLATED
import StringIO
import mmap
from tempfile import TemporaryFile, NamedTemporaryFile, SpooledTemporaryFile
import time
from os.path import getsize
from pkg_resources import resource_filename

from webob.request import Request
from paste.httpexceptions import HTTPBadRequest

from pdp_util.util import get_stn_list
from pydap.handlers.pcic import RawPcicSqlHandler, ClimoPcicSqlHandler
from pydap.handlers.lib import BaseHandler
from pydap.handlers.csv import CSVHandler
from pydap.handlers.sql import SQLHandler

from pdp_util.util import get_extension, get_clip_dates

class PcdsZipApp(object):
    '''WSGI application which accepts a set of PCDS filters in the request and responds with a generator which streams the OPeNDAP responses one by one
    '''
    def __init__(self, conn_params={}):
        self.conn_params = conn_params

    def __call__(self, environ, start_response):
        '''Fire off pydap requests and return an iterable (from :func:`ziperator`)'''
        stns = get_stn_list(environ, eval(self.conn_params))

        ext = get_extension(environ)
        if not ext:
            return HTTPBadRequest("Requested extension not supported")(environ, start_response)
                
        status = '200 OK'
        response_headers = [('Content-type','application/zip'), ('Content-Disposition', 'filename="pcds_data.zip"')]
        start_response(status, response_headers)
        environ['pydap.handlers.pcic.conn_params'] = self.conn_params

        responders = chain(get_metadata_index(stns, ext, environ),
                           get_pcds_responders(stns, ext, get_clip_dates(environ), environ)
                           )

        return ziperator(responders, ext)

    @staticmethod
    def send_file(file_path, BLOCK_SIZE):
        '''FIXME: Unused?
        '''
        with open(file_path) as f:
            block = f.read(BLOCK_SIZE)
            while block:
                yield block
                block = f.read(BLOCK_SIZE)

    @staticmethod
    def temp_zip(stations, responders, ext, start=time.time()):
        '''FIXME: Unused?
        '''
        with NamedTemporaryFile(delete=False) as f:
            zipify(f, stations, responders, ext, start)
        return f.name

    @staticmethod
    def stringio_zip(stations, responders, ext, start=time.time()):
        '''FIXME: Unused?
        '''
        buf = StringIO.StringIO()
        zipify(buf, stations, responders, ext, start)
        return buf.getvalue()

def ziperator(responders, ext, start=time.time()):
    '''This method creates and returns an iterator which yields bytes for a :py:class:`ZipFile` that contains a set of files from OPeNDAP requests. The method will spool the first one gigabyte in memory using a :py:class:`SpooledTemporaryFile`, after which it will use disk.

       :param responders: A list of (``name``, ``generator``) pairs where ``name`` is the filename to use in the zip archive and ``generator`` should yield all bytes for a single file.
       :param ext: FIXME: unused?
       :param start: FIXME: unused?
       :type start: time.time
       :rtype: iterator
    '''
    with SpooledTemporaryFile(1024*1024*1024) as f:
        yield 'PK' # Response headers aren't sent until the first chunk of data is sent.  Let's get this repsonse moving!
        z = ZipFile(f, 'w', ZIP_DEFLATED)

        for name, responder in responders:
            pos = 2 if f.tell() == 0 else f.tell()
            z.writestr(name, ''.join([x for x in responder]))
            f.seek(pos)
            yield f.read()
        pos = f.tell()
        z.close()
        f.seek(pos)
        yield f.read()
    
def zipify(open_file, stations, responders, ext, start=time.time()):
    '''FIXME: Unused?
    '''
    z = ZipFile(open_file, 'w', ZIP_DEFLATED)
    for (net, stn), responder in zip(stations, responders):
        name = '%(net)s/%(stn)s.%(ext)s' % locals()
        z.writestr(name, ''.join([x for x in responder]))
    z.close()
    return z

def get_metadata_index(stations, ext, environ):
    '''This function is a generator which yields (``name``, ``generator``) pairs where ``name`` is the filename (e.g. [``network_name``].csv) and ``generator`` streams a csv file with information on the network's variables
    
       :param stations: A list of (``network_name``, ``native_id``) pairs representing the stations for which this response should include variable metadata
       :param ext: FIXME: unused?
       :param environ:
       :rtype: iterator
    '''
    req = Request(environ)
    form = req.params
    meta_file = 'clim_vars' if 'download-climatology' in form else 'variables'

    nets = set(zip(*stations)[0])
    for net in nets:
        responder = SQLHandler(resource_filename(__name__, "data/%s.sql" % meta_file))
        # dirty hack to keep the config out of the code repo
        responder.config_lines[1] = '  dsn: "postgresql://%(user)s:%(password)s@%(host)s/%(database)s"\n' % eval(environ['pydap.handlers.pcic.conn_params'])

        name = '%(net)s/variables.csv' % locals()
        newenv = environ.copy()
        newenv['PATH_INFO'] = '/variables.sql.csv'
        newenv['QUERY_STRING'] = "variables.network='%s'" % net
        yield (name, responder(newenv, lambda x, y: None))


def get_pcds_responders(stns, extension, clip_dates, environ):
    '''Iterator object which coalesces a list of stations, compresses them, and returns the data for the response

    :param stations: A list of (``network_name``, ``native_id``) pairs representing the stations for which this response should include variable metadata
    :param extension: extension representing the response file type which should be appended to the request
    :type extension: str
    :param clip_dates: pair datetime.datetime objects representing the start and end times for which data should be returned (inclusive)
    :param environ:
    :type environ: dict
    :rtype: iterator
    '''
    req = Request(environ)
    form = req.params

    handle_class, handler_ext = (ClimoPcicSqlHandler, 'csql') if 'download-climatology' in form else (RawPcicSqlHandler, 'rsql')

    sdate, edate = clip_dates
    content_length = 0
    for net, stn in stns:
        handler = handle_class('/%s/%s.%s' % (net, stn, handler_ext))

        newenv = environ.copy()
        newenv['PATH_INFO'] = '/%s/%s.%s.%s' % (net, stn, handler_ext, extension)

        qs = []
        if sdate: qs.append("station_observations.time>='%s'" % sdate)
        if edate: qs.append("station_observations.time<='%s'" % edate)
        newenv['QUERY_STRING'] = '&'.join(qs)

        name = '%(net)s/%(stn)s.%(extension)s' % locals()
        yield (name, handler(newenv, lambda x, y: None))

def test_responders(environ):
    def my_start_response(*args):
        pass
    handler = CSVHandler('/home/hiebert/code/hg/data_portal_dev/server/data/136.csv')
    environ['PATH_INFO'] = '/136.csv'
    yield handler(environ, my_start_response)

def agg_generator(global_conf, **kwargs):
    '''Factory function for the :class:`PcdsZipApp`

       :param global_conf: dict containing the key conn_params which is passed on to :class:`PcdsZipApp`. Everything else is ignored.
       :param kwargs: ignored
    '''
    return PcdsZipApp(conn_params=global_conf['conn_params'])
