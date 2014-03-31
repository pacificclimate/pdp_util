from collections import OrderedDict

from webob.request import Request
from simplejson import dumps
from numpy import array

from pdp_util import session_scope
from modelmeta import *

class EnsembleMemberLister(object):
    # TODO: Measure performance with thousands of elements
    def __init__(self, dsn):
        def session_scope_factory():
            return session_scope(dsn)
        self.session_scope_factory = session_scope_factory

    def __call__(self, environ, start_response):
        req = Request(environ)
        try:
            ensemble_name = req.params['ensemble_name']
        except KeyError:
            start_response('400 Bad Request', [])
            return ["Required parameter 'ensemble_name' not specified"]

        with self.session_scope_factory() as sesh:
            ensemble = sesh.query(Ensemble).filter(Ensemble.name == ensemble_name).first()

            if not ensemble: # Result does not contain any row therefore ensemble does not exist
                start_response('200 OK', [('Content-type','text/plain; charset=utf-8')])
                return ['']

            tuples = [x for x in self.list_stuff(ensemble)] # query is lazy load, so must be assigned within scope

        status = '200 OK'
        response_headers = [('Content-type','application/json; charset=utf-8')]
        start_response(status, response_headers)
        d = OrderedDict(sorted(dictify(array(tuples)).items(), key=lambda t:t[0]))
        return dumps(d)

    def list_stuff(self, ensemble):
        for dfv in ensemble.data_file_variables:
            yield dfv.file.run.emission.short_name, dfv.file.run.model.short_name, dfv.netcdf_variable_name, dfv.file.unique_id.replace('+', '-')

class PrismEnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for dfv in ensemble.data_file_variables:
            yield dfv.file.run.model.short_name, dfv.netcdf_variable_name, dfv.file.unique_id.replace('+', '-')

class DownscaledEnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for dfv in ensemble.data_file_variables:
            yield dfv.file.run.emission.short_name, dfv.file.run.model.short_name, dfv.netcdf_variable_name, dfv.file.unique_id.replace('+', '-')

class VicGen1EnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for dfv in ensemble.data_file_variables:
            yield dfv.file.run.emission.short_name, dfv.file.run.model.short_name, dfv.netcdf_variable_name, dfv.file.unique_id.replace('+', '-')

class ClimdexEnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for dfv in ensemble.data_file_variables:
            ## FIXME
            yield dfv.file.run.emission.short_name, dfv.file.run.model.short_name, "annual" if "_yr_" in dfv.file.unique_id else "monthly", dfv.netcdf_variable_name, dfv.file.unique_id.replace('+', '-')

def dictify(a):
    if len(a) == 1 and len(a[0]) == 1:
        return a.flatten()[0]
    else:
        keys = set(a[:,0])
        return { key: dictify(a[ array([ val == key for val in a[:,0] ], dtype=bool) ,1:]) for key in keys }
