import logging

import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest

import pycds
import modelmeta
import pdp_util
from pdp_util.ensemble_members import EnsembleMemberLister
from pdp_util.raster import RasterMetadata


@pytest.yield_fixture(scope='function')
def blank_postgis_session():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        sesh = sessionmaker(bind=engine)()
        yield sesh


@pytest.yield_fixture(scope='function')
def test_session(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    pycds.DeferredBase.metadata.create_all(bind=engine)
    yield blank_postgis_session


@pytest.yield_fixture(scope='function')
def test_session_with_data(test_session):
    yield test_session


@pytest.fixture(scope='function')
def conn_params(test_session_with_data):
    return test_session_with_data.get_bind().url


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
