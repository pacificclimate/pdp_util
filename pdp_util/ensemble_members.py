from webob.request import Request
from simplejson import dumps
from numpy import array

import pdp_util
from modelmeta import *

class EnsembleMemberLister(object):
    # TODO: Measure performance with thousands of elements
    def __init__(self, dsn):
        self._session_factory = pdp_util.get_session(dsn)

    def __call__(self, environ, start_response):
        req = Request(environ)
        try:
            ensemble_name = req.params['ensemble_name']
        except KeyError:
            start_response('400 Bad Request', [])
            return ["Required parameter 'ensemble_name' not specified"]

        sesh = self._session_factory()
        ensemble = sesh.query(Ensemble).filter(Ensemble.name == ensemble_name).first() # FIXME: Could raise error

        status = '200 OK'
        response_headers = [('Content-type','application/json; charset=utf-8')]
        start_response(status, response_headers)
        if not ensemble:
            return []
        
        def list_stuff():
            for ensemble_run in ensemble.ensemble_runs:
                run = ensemble_run.run
                for file_ in run.files:
                    for data_file_variable in file_.data_file_variables:
                        yield run.emission.short_name, run.model.short_name, run.name, data_file_variable.netcdf_variable_name, file_.unique_id.replace('+', '-') # FIXME: kill the replacement once ncWMS stops being dumb

        tuples = [x for x in list_stuff()]

        def dictify(a):
            if len(a) == 1:
                return a.flatten()[0]
            else:
                keys = set(a[:,0])
                return { key: dictify(a[ array([ val == key for val in a[:,0] ], dtype=bool) ,1:]) for key in keys }

        sesh.close()
        return dumps(dictify(array(tuples)))
