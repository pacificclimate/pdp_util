"""
The pdp_util module
"""
from threading import Lock
from contextlib import contextmanager

from sqlalchemy import create_engine, exc, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import Pool

from pdp_util.dbdict import DbDict, dict_to_dsn

# module global dictionary of database sessions
DBPOOL = DbDict()
DBLOCK = Lock()


def get_session(dsn):
    """Function which provides module-level database sessions.

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
    """
    if isinstance(dsn, dict):
        dsn = dict_to_dsn(dsn)
    with DBLOCK:
        if dsn not in DBPOOL:
            engine = create_engine(dsn)
            Session = sessionmaker(bind=engine)
            DBPOOL[dsn] = Session
            return Session
    return DBPOOL[dsn]


# From http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html#session-faq-whentocreate
@contextmanager
def session_scope(dsn):
    """Provide a transactional scope around a series of operations."""
    factory = get_session(dsn)
    session = factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
    """This function is an event listener that "pings" (runs an inexpensive query and discards the results)
    each time a connection is checked out from the connection pool.
    If the ping fails, this method raises a DisconnectionError which forces the current connection to be disposed
    See: http://docs.sqlalchemy.org/en/rel_0_9/core/events.html for further details.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
    except:
        # raise DisconnectionError - pool will try
        # connecting again up to three times before raising.
        raise exc.DisconnectionError()
    cursor.close()


from paste.httpexceptions import HTTPNotFound


class Catcher(object):
    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        try:
            set_trace()
            return self.app(environ, start_response)
        except HTTPNotFound:
            set_trace()
