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
        for ensemble_run in ensemble.ensemble_runs:
            run = ensemble_run.run
            for file_ in run.files:
                for data_file_variable in file_.data_file_variables:
                    yield run.emission.short_name, run.model.short_name, run.name, data_file_variable.netcdf_variable_name, file_.unique_id.replace('+', '-') # FIXME: kill the replacement once ncWMS stops being dumb

class PrismEnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for ensemble_run in ensemble.ensemble_runs:
            run = ensemble_run.run
            for file_ in run.files:
                for data_file_variable in file_.data_file_variables:
                    yield run.model.short_name, data_file_variable.netcdf_variable_name, file_.unique_id.replace('+', '-') # FIXME: kill the replacement once ncWMS stops being dumb

class DownscaledEnsembleLister(EnsembleMemberLister):
    def list_stuff(self, ensemble):
        for ensemble_run in ensemble.ensemble_runs:
            run = ensemble_run.run
            for file_ in run.files:
                for data_file_variable in file_.data_file_variables:
                    yield run.emission.short_name, run.model.short_name, data_file_variable.netcdf_variable_name, file_.unique_id.replace('+', '-') # FIXME: kill the replacement once ncWMS stops being dumb

def dictify(a):
    if len(a) == 1:
        return a.flatten()[0]
    else:
        keys = set(a[:,0])
        return { key: dictify(a[ array([ val == key for val in a[:,0] ], dtype=bool) ,1:]) for key in keys }
