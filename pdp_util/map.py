'''An application for serving a map
'''

from genshi.template import TemplateLoader

from pdp_util.auth import providers

class MapApp(object):
    def __init__(self, **kwargs):
        '''Initialize the MapApp
        
        :param root: The absolute URL of where this application lives
        :param gs_url: Absolute URL to a GeoServer instance
        :param templates: filesystem path to where the templates live
        :param version: project version string
        :rtype: MapApp
        '''
        required_args = set(['app_root',
                            'templates',
                            'version',
                            'title'])
        if not required_args.issubset(kwargs.keys()):
                raise ValueError("Some required arguments are missing {}".format(required_args - set(kwargs.keys())))
        self.options = kwargs

    def __call__(self, environ, start_response):
        '''Call the MapApp, start the response, and generate the content'''
        status = '200 OK'
        response_headers = [('Content-type', 'text/html; charset=utf-8')]
        start_response(status, response_headers)

        template = 'map.html'
        context = {
            'environ': environ,
            'providers': providers,
        }
        context.update(self.options)
        return self.render(**context)

    def render(self, **kwargs):
        loader = TemplateLoader(self.options['templates'])
        tmpl = loader.load('map.html')
        stream = tmpl.generate(**kwargs)
        return stream.render('html', doctype='html', encoding='utf-8')
