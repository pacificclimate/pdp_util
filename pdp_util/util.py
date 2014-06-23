from datetime import datetime

from webob.request import Request

from pdp_util.filters import form_filters
from pydap.responses.lib import load_responses
from pycds import CrmpNetworkGeoserver as cng

def get_stn_list(sesh, sql_constraints, to_select = [cng.network_name, cng.native_id]):
    '''Translate station filters into a list of stations

       :param sesh: The SQLAlchemy database session
       :type sesh: :py:class:`sqlalchemy.orm.session.Session`
       :param sql_constraints: A list of filters which can filter a sqlalchemy :py:class:`Query` object. This can be a precompiled sqlalchemy.sql.expression or the string of a WHERE clause.
       :param to_select: A list of ORM columns to select
    '''
    try:
        len(to_select)
        q = sesh.query(*to_select)
    except:
        q = sesh.query(to_select)
    for constraint in sql_constraints:
        q = q.filter(constraint)

    return q.all()

def get_extension(environ):
    '''Extract the data format extension from request parameters and check that they are supported'''
    req = Request(environ)
    form = req.params
    if form.has_key('data-format') and form['data-format'] in load_responses().keys():
        return form['data-format']
    else:
        return None

def get_clip_dates(environ):
    '''Extract dates from request parameters

       :param environ: WSGI request environment dictionary
       :rtype tuple of datetimes (start_date, end_date) or Nones
    '''
    req = Request(environ)
    form = req.params
    if not form.has_key('cliptodate'):
        return (None, None)
    else:
        sdate = form['from-date'] if form.has_key('from-date') else ''
        edate = form['to-date'] if form.has_key('to-date') else ''
        sdate = form_filters['from-date'].validate(sdate)
        edate = form_filters['to-date'].validate(edate)
        return (datetime.strptime(sdate, '%Y/%m/%d') if sdate else None, datetime.strptime(edate, '%Y/%m/%d') if edate else None)

