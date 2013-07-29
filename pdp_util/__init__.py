"""
The pdp_util module
"""
from threading import Lock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from pdp_util.dbdict import DbDict, dict_to_dsn

# module global dictionary of database sessions
DBPOOL = DbDict()
DBLOCK = Lock()
def get_session(dsn):
    '''Function which provides module-level database sessions.

       This function creates an sqlalchemy database engine and session factory for a given dsn. If the session factory has not yet been created for this invocation of the program, it will create it and store it at the module level. Subsequent invocations of this function with the same arguments will return a new session factory for the same engine. The engine takes care of connection pooling and everything. See sqlalchemy docs for more details.

       Example::

        from pcic import get_session
        conn_params = {'database': 'crmp', 'user': 'hiebert', 'host': 'monsoon.pcic.uvic.ca'}
        session_factory = get_session(conn_params)
        session = session_factory()
        query = session.execute("SELECT sum(count) FROM climo_obs_count_mv NATURAL JOIN meta_history WHERE ARRAY[station_id] <@ :stns", {'stns': [100, 203]})
        query.fetchone()

        next_session_factory = get_session(conn_params)
        next_session_factory == session_factory # Returns True
        next_session_factory() == session # Returns False
        
       :param dsn: dict or sqlalchemy-style dns string
       :rtype: session factory
    '''
    if isinstance(dsn, dict):
        dsn = dict_to_dsn(dsn)
    with DBLOCK:
        if dsn not in DBPOOL:
            engine = create_engine(dsn)
            Session = sessionmaker(bind=engine)
            DBPOOL[dsn] = Session
            return Session
    return DBPOOL[dsn]

from paste.httpexceptions import HTTPNotFound

class Catcher(object):
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        try:
            set_trace()
            return self.app(environ, start_response)
        except HTTPNotFound, e:
            set_trace()
