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


@pytest.yield_fixture(scope='function')
def test_session_with_unpublished(test_session):
    unpub = pycds.Network(name='MoAG')
    stns = [
        pycds.Station(
            native_id='does not matter', network=unpub,
                histories=[
                    pycds.History(
                        station_name='Does Not Matter',
                        the_geom='SRID=4326;POINT(-140.866667 62.416667)'
                    )
                ]
        )
    ]
    test_session.add_all(stns + [unpub])
    yield test_session
