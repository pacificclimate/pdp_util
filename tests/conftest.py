import logging
from pkg_resources import resource_filename

import testing.postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pytest

import pycds
import modelmeta
from pdp_util.ensemble_members import EnsembleMemberLister
from pdp_util.raster import RasterMetadata


@pytest.yield_fixture(scope='function')
def blank_postgis_session():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        yield sessionmaker(bind=engine)()


@pytest.yield_fixture(scope='function')
def test_session(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    pycds.DeferredBase.metadata.create_all(bind=engine)
    yield blank_postgis_session


@pytest.yield_fixture(scope='function')
def test_session_with_data(blank_postgis_session):
    logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
    engine = blank_postgis_session.get_bind()
    pycds.Base.metadata.create_all(bind=engine)
    pycds.DeferredBase.metadata.create_all(bind=engine)

    with open(resource_filename('pycds', 'data/crmp_subset_data.sql'), 'r') as f:
        sql = f.read()
    blank_postgis_session.execute(sql)

    yield blank_postgis_session


@pytest.yield_fixture(scope='function')
def conn_params(test_session_with_data):
    yield test_session_with_data.get_bind().url


@pytest.yield_fixture(scope='function')
def conn_params():
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        sesh = sessionmaker(bind=engine)()
        logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)
        pycds.Base.metadata.create_all(bind=engine)
        pycds.DeferredBase.metadata.create_all(bind=engine)
        with open(resource_filename('pycds', 'data/crmp_subset_data.sql'), 'r') as f:
            sql = f.read()
            sesh.execute(sql)
            sesh.commit()
        yield pg.url()

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
