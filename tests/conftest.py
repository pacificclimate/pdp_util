import logging
from pkg_resources import resource_filename

import pytest
import pycds
from pycds import Network, Contact, Station, History, Variable
import modelmeta
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
import testing.postgresql

import pdp_util
from pdp_util.ensemble_members import EnsembleMemberLister
from pdp_util.raster import RasterMetadata

@pytest.fixture(scope='session')
def engine():
    """Test-session-wide database engine"""
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('crmp'))
        pycds.Base.metadata.create_all(bind=engine)
        yield engine


@pytest.fixture(scope='session')
def empty_session(engine):
    """Single-test database session. All session actions are rolled back on teardown"""
    session = sessionmaker(bind=engine)()
    # Default search path is `"$user", public`. Need to reset that to search crmp (for our db/orm content) and
    # public (for postgis functions)
    session.execute('SET search_path TO crmp, public')
    # print('\nsearch_path', [r for r in session.execute('SHOW search_path')])
    yield session
    session.rollback()
    session.close()


@pytest.fixture(scope='session')
def test_session(empty_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

    empty_session.begin_nested()
    with open(resource_filename('pycds', 'data/crmp_subset_data.sql'), 'r') as f:
        sql = f.read()
    empty_session.execute(sql)
    empty_session.commit()

    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO) # Let's not log all the db setup stuff...

    yield empty_session

    empty_session.rollback()
    empty_session.close()


@pytest.fixture(scope="function")
def conn_params(test_session):
    yield test_session.get_bind().url


@pytest.fixture(scope="function")
def ensemble_member_lister():
    return EnsembleMemberLister(modelmeta.test_dsn)

@pytest.fixture(scope="function")
def raster_metadata():
    return RasterMetadata(modelmeta.test_dsn)

@pytest.fixture(scope="function")
def mm_session():
    return modelmeta.test_session()

@pytest.fixture(scope="function")
def mm_dsn():
    return modelmeta.test_dsn
