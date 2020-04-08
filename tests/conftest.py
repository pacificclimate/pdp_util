import datetime
import logging
from pkg_resources import resource_filename

import pytest
import pycds
from pycds import Network, Contact, Station, History, Variable
import modelmeta
from modelmeta import (
    Ensemble,
    DataFile,
    DataFileVariableDSGTimeSeries,
    VariableAlias,
    EnsembleDataFileVariables,
)
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


#######################################################################
# Test fixtures for code dependent on modelmeta database

# TODO: Factor out common engine creation
@pytest.fixture(scope='session')
def mm_engine():
    """Test-session-wide database engine"""
    with testing.postgresql.Postgresql() as pg:
        engine = create_engine(pg.url())
        engine.execute("create extension postgis")
        engine.execute(CreateSchema('test_meta'))
        modelmeta.Base.metadata.create_all(bind=engine)
        yield engine
        engine.dispose()


@pytest.fixture(scope='function')
def mm_empty_session(mm_engine):
    """Single-test database session. All session actions are rolled back on teardown"""
    session = sessionmaker(bind=mm_engine)()
    # Default search path is `"$user", public`. Need to reset that to search crmp (for our db/orm content) and
    # public (for postgis functions)
    session.execute('SET search_path TO test_meta, public')
    # print('\nsearch_path', [r for r in session.execute('SHOW search_path')])
    yield session
    session.rollback()
    session.close()


# Ensemble

def make_ensemble(id):
    return Ensemble(
        id=id,
        changes='wonder what this is for',
        description='Ensemble {}'.format(id),
        name='ensemble{}'.format(id),
        version=float(id)
    )


@pytest.fixture(scope='function')
def ensemble1():
    return make_ensemble(1)


@pytest.fixture(scope='function')
def ensemble2():
    return make_ensemble(2)


# DataFile

def make_data_file(i, run=None, timeset=None):
    return DataFile(
        id=i,
        filename='/storage/data_file_{}'.format(i),
        first_1mib_md5sum='first_1mib_md5sum',
        unique_id='unique_id_{}'.format(i),
        x_dim_name='lon',
        y_dim_name='lat',
        t_dim_name='time',
        index_time=datetime.datetime.now(),
        run=run,
        timeset=timeset,
    )


@pytest.fixture(scope='function')
def data_file_1():
    return make_data_file(1)


@pytest.fixture(scope='function')
def data_file_2():
    return make_data_file(2)


@pytest.fixture(scope='function')
def data_file_3():
    return make_data_file(3)


# VariableAlias

def make_variable_alias(i):
    return VariableAlias(
        long_name='long_name_{}'.format(i),
        standard_name='standard_name_{}'.format(i),
        units='units_{}'.format(i),
    )


@pytest.fixture(scope='function')
def variable_alias_1():
    return make_variable_alias(1)


@pytest.fixture(scope='function')
def variable_alias_2():
    return make_variable_alias(2)


# DataFileVariableDSGTimeSeries

def make_test_dfv_dsg_time_series(i, file=None, variable_alias=None):
    return DataFileVariableDSGTimeSeries(
        id=i,
        derivation_method='derivation_method_{}'.format(i),
        variable_cell_methods='variable_cell_methods_{}'.format(i),
        netcdf_variable_name='var_{}'.format(i),
        disabled=False,
        range_min=0,
        range_max=100,
        file=file,
        variable_alias=variable_alias,
    )


@pytest.fixture(scope='function')
def dfv_dsg_time_series_11(data_file_1, variable_alias_1):
    return make_test_dfv_dsg_time_series(
        1, file=data_file_1, variable_alias=variable_alias_1)


@pytest.fixture(scope='function')
def dfv_dsg_time_series_12(data_file_1, variable_alias_2):
    return make_test_dfv_dsg_time_series(
        2, file=data_file_1, variable_alias=variable_alias_2)


@pytest.fixture(scope='function')
def dfv_dsg_time_series_21(data_file_2, variable_alias_1):
    return make_test_dfv_dsg_time_series(
        3, file=data_file_2, variable_alias=variable_alias_1)


@pytest.fixture(scope='function')
def dfv_dsg_time_series_31(data_file_3, variable_alias_1):
    return make_test_dfv_dsg_time_series(
        4, file=data_file_3, variable_alias=variable_alias_1)


# EnsembleDataFileVariables: tag DFVs with Ensembles

def make_ensemble_dfvs(ensemble, dfv):
    return EnsembleDataFileVariables(
        ensemble_id=ensemble.id,
        data_file_variable_id=dfv.id,
    )


@pytest.fixture(scope='function')
def ensemble_dfvs_1(ensemble1, dfv_dsg_time_series_11, dfv_dsg_time_series_21):
    return [
        make_ensemble_dfvs(ensemble1, dfv_dsg_time_series_11),
        make_ensemble_dfvs(ensemble1, dfv_dsg_time_series_21),
    ]


# TODO: Make this, or something like it, drive ensemble_dfvs_1
@pytest.fixture(scope='function')
def ensemble1_data_files(data_file_1, data_file_2):
    return {data_file_1, data_file_2}


@pytest.fixture(scope='function')
def ensemble_dfvs_2(ensemble2, dfv_dsg_time_series_21, dfv_dsg_time_series_31):
    return [
        make_ensemble_dfvs(ensemble2, dfv_dsg_time_series_21),
        make_ensemble_dfvs(ensemble2, dfv_dsg_time_series_31),
    ]


# TODO: Make this, or something like it, drive ensemble_dfvs_2
@pytest.fixture(scope='function')
def ensemble2_data_files(data_file_3, data_file_2):
    return {data_file_2, data_file_3}


# Database sessions

@pytest.fixture(scope="function")
def mm_test_session(
    mm_empty_session,
    ensemble1,
    ensemble2,
    dfv_dsg_time_series_11,
    dfv_dsg_time_series_12,
    ensemble_dfvs_1,
    ensemble_dfvs_2,
):
    s = mm_empty_session
    s.add_all([ensemble1, ensemble2])
    s.flush()
    s.add_all([dfv_dsg_time_series_11, dfv_dsg_time_series_12])
    s.flush()
    s.add_all(ensemble_dfvs_1)
    s.add_all(ensemble_dfvs_2)
    s.flush()
    yield s


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
