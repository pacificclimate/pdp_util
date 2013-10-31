from datetime import timedelta

import json
from webob.request import Request
from sqlalchemy.sql.expression import select
from sqlalchemy import func, text
from dateutil.relativedelta import relativedelta

from pycds import CrmpNetworkGeoserver as cng, History, ObsCountPerMonthHistory, ClimoObsCount
from pdp_util.util import get_stn_list, get_clip_dates
from pdp_util.filters import validate_vars
from pdp_util import session_scope

class CountStationsApp(object):
    '''Application for counting the number of stations that meet the query parameters
    '''
    def __init__(self, conn_params):
        def session_scope_factory():
            return session_scope(conn_params)
        self.session_scope_factory = session_scope_factory

    def __call__(self, environ, start_response):
        status = '200 OK'
        response_headers = [('Content-type', 'application/json; charset=utf-8')]
        start_response(status, response_headers)

        filters = validate_vars(environ)

        with self.session_scope_factory() as sesh:
            stns = get_stn_list(sesh, filters)
        return json.dumps({'stations_selected': len(stns)})

class CountRecordLengthApp(object):
    '''Applications for estimating the length of the dataset which would be returned
       by the stations which meet the given criteria
    '''
    def __init__(self, conn_params, max_stns):
        self.max_stns = int(max_stns)

        def session_scope_factory():
            return session_scope(conn_params)
        self.session_scope_factory = session_scope_factory


    def __call__(self, environ, start_response):
        req = Request(environ)
        form = req.params

        filters = validate_vars(environ)
        sdate, edate = get_clip_dates(environ)

        with self.session_scope_factory() as sesh:
            stns = [stn[0] for stn in get_stn_list(sesh, filters, cng.station_id)]

        rv = length_of_return_dataset(sesh, stns, sdate, edate)
        obs_count = int(rv[0] if rv[0] else 0)
        rv = length_of_return_climo(sesh, stns)
        climo_count = int(rv[0] if rv[0] else 0)

        status = '200 OK'
        response_headers = [('Content-type', 'application/json; charset=utf-8')]
        start_response(status, response_headers)

        return json.dumps({'record_length': obs_count, 'climo_length': climo_count})

def length_of_return_dataset(sesh, stn_ids, sdate=None, edate=None):
    q = sesh.query(func.sum(ObsCountPerMonthHistory.count)).join(History, History.id == ObsCountPerMonthHistory.history_id).filter(History.station_id.in_(stn_ids))
    if sdate:
        sdate = sdate.replace(hour=0, minute=0, second=0, microsecond=0)
        q = q.filter(ObsCountPerMonthHistory.date_trunc >= sdate)
    if edate:
        edate = edate.replace(hour=0, minute=0, second=0, microsecond=0) + relativedelta(months=1)
        q = q.filter(ObsCountPerMonthHistory.date_trunc <= edate)

    return sesh.execute(q).fetchone()

def length_of_return_climo(sesh, stn_ids):
    q = sesh.query(func.sum(ClimoObsCount.count)).join(History, History.id == ClimoObsCount.history_id).filter(History.station_id.in_(stn_ids))
    return sesh.execute(q).fetchone()

if __name__ == '__main__':
    port = 8555
    from flask import Flask
    conn_params = {'database': 'crmp', 'user': 'hiebert', 'host': 'monsoon.pcic.uvic.ca'}
    main = CountRecordLengthApp(conn_params, 100)
    app = Flask(__name__)
    app.wsgi_app = main
    app.debug = True
    app.run('0.0.0.0', port, use_reloader=True, debug=True, use_debugger=True)
