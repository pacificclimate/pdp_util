import re, os
from StringIO import StringIO
import datetime
from pkg_resources import resource_filename

from pdp_util import session_scope
from pycds import Network

import pytz
from PIL import Image
from paste.httpexceptions import HTTPNotFound, HTTPNotModified
from webob.request import Request

class LegendApp(object):
    '''WSGI app that creates symbols for the network legend

       Each station on the PCDS map is colored by the network attribute and the colors are stored in the crmp database. This app queries the color table on instantiation and then responds to requests with a png with the appropriate color. As such, if the database is updated during run time, the changes will *not* take effect. The app will always set the Last-Modified header to be the time of instantiation.

       Network name is determined from the PATH_INFO matching ``[network_name].png``. ``network_name`` must be the lower case of the actual network_name attribute. For example ``PATH_INFO = motie.png`` will return the symbol for the MoTIe network. If the network is not found, a white symbol is returned.

       This app checks for the HTTP If-Modified-Since header and returns a 304 Not Modified response if possible.
    '''
    load_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    def __init__(self, conn_params):
        '''
           :param conn_params:
           :type conn_params: dict
        '''
        self.colors = {}
        with session_scope(conn_params) as sesh:
            q = sesh.query(Network.name, Network.color)
            for net, col in q:
                self.colors[net.lower()] = col

    def __call__(self, environ, start_response):
        req = Request(environ)

        m = re.match('.*/([a-zA-Z-_ ]+)\.(png)', environ.get('PATH_INFO', ''))
        if not m:
            return HTTPNotFound("PATH %s not found" % environ.get('PATH_INFO', ''))(environ, start_response) # 404
        network, ext = m.groups()

        # If they have it cached, tell 'em
        if req.if_modified_since and self.load_time <= req.if_modified_since:
            return HTTPNotModified()(environ, start_response) # 304

        if network.lower() in self.colors:
            color = self.colors[network.lower()]
        else:
            color = 'white'

        resource_file = resource_filename('pdp_util', 'data/alpha.png')
        alpha = Image.open(resource_file)
        _, _, _, a = alpha.split()
        r, g, b =  Image.new('RGB', alpha.size, color).split()
        outim = Image.merge(alpha.mode, [r,g,b,a])
        buf = StringIO()
        outim.save(buf, format='png')
        contents = buf.getvalue()

        status = '200 OK'
        response_headers = [('Last-Modified', self.load_time.strftime('%a, %d %b %Y %H:%M:%S GMT')),
                            ('Content-type', 'application/png'),
                            ('Content-length', str(len(contents)))]
        start_response(status, response_headers)
        return [contents]
