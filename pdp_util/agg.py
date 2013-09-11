'''
This module provides aggregation utilities to translate a single HTTP request into multiple OPeNDAP requests, returning a single response
'''

from itertools import chain
from zipfile import ZipFile, ZIP_DEFLATED
from tempfile import SpooledTemporaryFile

from webob.request import Request
from paste.httpexceptions import HTTPBadRequest
import numpy as np
from sqlalchemy import or_, not_
from sqlalchemy.orm import sessionmaker

from pdp_util.util import get_stn_list
from pdp_util.filters import validate_vars
from pycds import Variable, Network
from pydap.handlers.pcic import RawPcicSqlHandler, ClimoPcicSqlHandler
from pydap.handlers.sql import Engines
from pydap.model import DatasetType, SequenceType, BaseType
from pydap.handlers.lib import BaseHandler

from pdp_util.util import get_extension, get_clip_dates

class PcdsZipApp(object):
    '''WSGI application which accepts a set of PCDS filters in the request and responds with a generator which streams the OPeNDAP responses one by one
    '''
    def __init__(self, dsn, sesh=None):
        self.dsn = dsn
        if sesh:
            # Stash a copy of our engine in pydap.handlers.sql so that it will use it for data queries
            Engines[self.dsn] = sesh.get_bind()

    @property
    def session(self):
        Session = sessionmaker(bind=Engines[self.dsn])
        return Session()
            
    def __call__(self, environ, start_response):
        '''Fire off pydap requests and return an iterable (from :func:`ziperator`)'''
        req = Request(environ)
        form = req.params
        climo = True if 'download-climatology' in form else False

        filters = validate_vars(environ)
        stns = get_stn_list(self.session, filters)

        ext = get_extension(environ)
        if not ext:
            return HTTPBadRequest("Requested extension not supported")(environ, start_response)
                
        status = '200 OK'
        response_headers = [('Content-type','application/zip'), ('Content-Disposition', 'filename="pcds_data.zip"')]
        start_response(status, response_headers)
        environ['pydap.handlers.pcic.dsn'] = self.dsn

        responders = chain(get_all_metadata_index_responders(self.session, stns, climo),
                           get_pcds_responders(self.dsn, stns, ext, get_clip_dates(environ), environ)
                           )

        return ziperator(responders)

def ziperator(responders):
    '''This method creates and returns an iterator which yields bytes for a :py:class:`ZipFile` that contains a set of files from OPeNDAP requests. The method will spool the first one gigabyte in memory using a :py:class:`SpooledTemporaryFile`, after which it will use disk.

       :param responders: A list of (``name``, ``generator``) pairs where ``name`` is the filename to use in the zip archive and ``generator`` should yield all bytes for a single file.
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
    
def get_all_metadata_index_responders(sesh, stations, climo=False):
    '''This function is a generator which yields (``name``, ``generator``) pairs where ``name`` is the filename (e.g. [``network_name``].csv) and ``generator`` streams a csv file with information on the network's variables
    
    :param stations: A list of (``network_name``, ``native_id``) pairs representing the stations for which this response should include variable metadata
    :param climo: Should these be climatological variables?
    :type climo: bool
    :rtype: iterator
    '''
    nets = set(zip(*stations)[0])
    for net in nets:
        filename = '{0}/variables.csv'.format(net)
        yield (filename, metadata_index_responder(sesh, net, climo))

def metadata_index_responder(sesh, network, climo=False):
    '''The function creates a pydap csv response which lists variable metadata out of the database. It returns an generator for the contents of the file
    
    :param sesh: database session
    :type sesh: sqlalchemy.orm.session.Session
    :param network: Name of the network for which variables should be listed
    :type network: str
    :rtype: generator
    '''
    maxlen = 256
    if climo:
        climo_filt = or_(Variable.cell_method.like('%within%'), Variable.cell_method.like('%over%'))
    else:
        climo_filt = not_(or_(Variable.cell_method.like('%within%'), Variable.cell_method.like('%over%')))

    rv = sesh.query(Variable).join(Network).filter(Network.name == network).filter(climo_filt)
    a = np.array([(var.name, var.standard_name, var.cell_method, var.unit) for var in rv],
                 dtype=np.dtype({'names': ['variable', 'standard_name', 'cell_method', 'unit'],
                                 'formats':[(str, maxlen), (str, maxlen), (str, maxlen), (str, maxlen)]})
        )
    dst = DatasetType('Variable metadata')
    seq = SequenceType('variables')
    seq['variable']      = BaseType('variable')
    seq['standard_name'] = BaseType('standard_name', reference='http://llnl.gov/')
    seq['cell_method']   = BaseType('cell_method', reference='http://llnl.gov/')
    seq['unit']          = BaseType('unit')
    seq.data = a
    dst['variables'] = seq
    responder = BaseHandler(dst)
    
    environ = {'PATH_INFO': '/variables.foo.ascii', 'REQUEST_METHOD': 'GET'}
    return responder(environ, lambda x, y: None)

def get_pcds_responders(dsn, stns, extension, clip_dates, environ):
    '''Iterator object which coalesces a list of stations, compresses them, and returns the data for the response

    :param dsn:
    :param stations: A list of (``network_name``, ``native_id``) pairs representing the stations for which this response should include variable metadata
    :param extension: extension representing the response file type which should be appended to the request
    :type extension: str
    :param clip_dates: pair datetime.datetime objects representing the start and end times for which data should be returned (inclusive)
    :param environ: WSGI environment variables which optionally set the ``download-climatology`` field
    :type environ: dict
    :rtype: iterator
    '''
    req = Request(environ)
    form = req.params

    handler, handler_ext = (ClimoPcicSqlHandler(dsn), 'csql') if 'download-climatology' in form else (RawPcicSqlHandler(dsn), 'rsql')

    sdate, edate = clip_dates
    content_length = 0
    for net, stn in stns:
        newenv = environ.copy()
        newenv['PATH_INFO'] = '/%s/%s.%s.%s' % (net, stn, handler_ext, extension)

        qs = []
        if sdate: qs.append("station_observations.time>='%s'" % sdate)
        if edate: qs.append("station_observations.time<='%s'" % edate)
        newenv['QUERY_STRING'] = '&'.join(qs)

        name = '%(net)s/%(stn)s.%(extension)s' % locals()
        yield (name, handler(newenv, lambda x, y: None))

def agg_generator(global_conf, **kwargs):
    '''Factory function for the :class:`PcdsZipApp`

       :param global_conf: dict containing the key conn_params which is passed on to :class:`PcdsZipApp`. Everything else is ignored.
       :param kwargs: ignored
    '''
    return PcdsZipApp(dsn=global_conf['dsn'])
