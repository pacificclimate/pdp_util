"""This pair of tests demonstrates how connection pooling works and how to write a Pool event listener
   that pings the database with a short query before running the actual query.

   To run this test, configure pytest to discover it by adding a glob to the python_files discovery option.
   Create a setup.cfg ini file in the package directory that has these contents ::
       [pytest]
       python_files=manual_test*.py test*.py

   More details here: https://pytest.org/latest/example/pythoncollection.html
   When that is done, run the tests as follows ::

       $ path/to/pyenv/bin/python -m pytest -k restart -s -v

   Manually restart the postgres server in the middle of the test when prompted
"""

from contextlib import contextmanager

import pytest
from sqlalchemy import exc, event
from sqlalchemy.pool import Pool

from pdp_util import get_session

# These tests are specific to postgresql
@pytest.fixture
def test_dsn():
    return {"database": "pcic_meta", "user": "hiebert", "host": "atlas.pcic"}


@contextmanager
def session_scope(dsn):
    """Provide a transactional scope around a series of operations. But, for the purposes of testing,
    does not re-raise exceptions (unlike pdp_util.session_scope
    """
    factory = get_session(dsn)
    session = factory()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        # raise
    finally:
        session.close()


def test_restart_fails(test_dsn):
    """This isn't so much a test, as a demonstration of how the connection pooling works (and how it doesn't)
    in the context of a sqlalchmemy engine
    First we execute a query, then we restart the server, drop the connection and attempt to execute another query.
    The second query fails and the connection pool drops all of it's connections and reconnects.
    The third query succeeds, since the pool and been reestablished.
    """
    with session_scope(test_dsn) as sesh:
        sesh.execute("select 1")

    print("\nManually restart the server in another terminal")
    raw_input()

    # NOTE HERE that the first operation on the session raises an exception...
    with pytest.raises(exc.OperationalError) as excinfo:
        sesh = get_session(test_dsn)()
        sesh.execute("select 1")
        sesh.close()
    assert (
        "terminating connection due to administrator command" in excinfo.value.message
    )

    # ... but subsequent operations do not
    for _ in xrange(10):
        with session_scope(test_dsn) as sesh:
            sesh.execute("select 1")

    assert True


def test_restart_is_handled(test_dsn):
    @event.listens_for(Pool, "checkout")
    def ping_connection(dbapi_connection, connection_record, connection_proxy):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SELECT 1")
        except:
            # optional - dispose the whole pool
            # instead of invalidating one at a time
            # connection_proxy._pool.dispose()

            # raise DisconnectionError - pool will try
            # connecting again up to three times before raising.
            raise exc.DisconnectionError()
        cursor.close()

    for _ in range(3):
        with session_scope(test_dsn) as sesh:
            sesh.execute("select 1")

    print("\nManually restart the server in another terminal")
    raw_input()

    for _ in xrange(10):
        with session_scope(test_dsn) as sesh:
            sesh.execute("select 1")

    assert True
