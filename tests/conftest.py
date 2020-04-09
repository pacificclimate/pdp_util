import datetime
import logging
import json
from pkg_resources import resource_filename
from webob.request import Request

import pytest
import pycds
from pycds import Network, Contact, Station, History, Variable
import modelmeta
from modelmeta import (
    Model,
    Emission,
    Run,
    DataFile,
    DataFileVariableDSGTimeSeries,
    VariableAlias,
    Ensemble,
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
def mm_database_dsn():
    """Test-session-wide testing.Postgresql instance; returns dsn for it"""
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope='session')
def mm_engine(mm_database_dsn):
    """Test-session-wide database engine"""
    engine = create_engine(mm_database_dsn)
    engine.execute("create extension postgis")
    engine.execute(CreateSchema('test_meta'))
    modelmeta.Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope='function')
def mm_empty_session(mm_engine):
    """Single-test database session.
    All session actions are rolled back on teardown"""
    session = sessionmaker(bind=mm_engine)()
    session.execute('SET search_path TO test_meta, public')
    yield session
    session.rollback()
    session.close()


# Database test objects

# Overview
#
# We set up the following objects which can be added to the database:
#
# model_1: Model
#
# emission_1: Emission
# emission_2: Emission
#
# run_11: Run(model_1, emission_1)
# run_12: Run(model_1, emission_2)
#
# data_file_1: DataFile(run_11)
# data_file_2: DataFile(run_12)
# data_file_3: DataFile(run_11)
#
# variable_alias_1: VariableAlias
# variable_alias_2: VariableAlias
#
# dfv_dsg_time_series_11:
#   DataFileVariableDSGTimeSeries(data_file_1, variable_alias_1)
# dfv_dsg_time_series_12:
#   DataFileVariableDSGTimeSeries(data_file_1, variable_alias_2)
# dfv_dsg_time_series_21:
#   DataFileVariableDSGTimeSeries(data_file_2, variable_alias_2)
# dfv_dsg_time_series_31:
#   DataFileVariableDSGTimeSeries(data_file_3, variable_alias_2)
#
# ensemble_1: Ensemble
#   dfv_dsg_time_series_11
#   dfv_dsg_time_series_21
#
# ensemble_2: Ensemble
#   dfv_dsg_time_series_21
#   dfv_dsg_time_series_31


# Model

def make_model(i):
    return Model(
        id=i,
        short_name='model_{}'.format(i),
        type='model_type'
    )


@pytest.fixture(scope='function')
def model_1():
    return make_model(1)


# Emission

def make_emission(i):
    return Emission(
        id=i,
        short_name='emission_{}'.format(i),
    )


@pytest.fixture(scope='function')
def emission_1():
    return make_emission(1)


@pytest.fixture(scope='function')
def emission_2():
    return make_emission(2)


# Run

def make_run(i, model, emission):
    return Run(
        id=i,
        name='emission_{}'.format(i),
        model_id=model.id,
        emission_id=emission.id,
    )


@pytest.fixture(scope='function')
def run_12(model_1, emission_1):
    return make_run(1, model_1, emission_1)


@pytest.fixture(scope='function')
def run_11(model_1, emission_2):
    return make_run(2, model_1, emission_2)


# DataFile

def make_data_file(i, run=None, timeset=None):
    return DataFile(
        id=i,
        filename='/storage/data_file_{}.nc'.format(i),
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
def data_file_1(run_11):
    return make_data_file(1, run_11)


@pytest.fixture(scope='function')
def data_file_2(run_12):
    return make_data_file(2, run_12)


@pytest.fixture(scope='function')
def data_file_3(run_11):
    return make_data_file(3, run_11)


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


# Ensemble

def make_ensemble(id):
    return Ensemble(
        id=id,
        changes='wonder what this is for',
        description='Ensemble {}'.format(id),
        name='ensemble_{}'.format(id),
        version=float(id)
    )


@pytest.fixture(scope='function')
def ensemble_1():
    return make_ensemble(1)


@pytest.fixture(scope='function')
def ensemble_2():
    return make_ensemble(2)

# EnsembleDataFileVariables: tag DFVs with Ensembles

def make_ensemble_dfvs(ensemble, dfv):
    return EnsembleDataFileVariables(
        ensemble_id=ensemble.id,
        data_file_variable_id=dfv.id,
    )


@pytest.fixture(scope='function')
def ensemble_dfvs_1(ensemble_1, dfv_dsg_time_series_11, dfv_dsg_time_series_21):
    return [
        make_ensemble_dfvs(ensemble_1, dfv_dsg_time_series_11),
        make_ensemble_dfvs(ensemble_1, dfv_dsg_time_series_21),
    ]


# TODO: Make this, or something like it, drive ensemble_dfvs_1
@pytest.fixture(scope='function')
def ensemble_1_data_files(data_file_1, data_file_2):
    return {data_file_1, data_file_2}


@pytest.fixture(scope='function')
def ensemble_dfvs_2(ensemble_2, dfv_dsg_time_series_21, dfv_dsg_time_series_31):
    return [
        make_ensemble_dfvs(ensemble_2, dfv_dsg_time_series_21),
        make_ensemble_dfvs(ensemble_2, dfv_dsg_time_series_31),
    ]


# TODO: Make this, or something like it, drive ensemble_dfvs_2
@pytest.fixture(scope='function')
def ensemble_2_data_files(data_file_3, data_file_2):
    return {data_file_2, data_file_3}


# Database sessions

@pytest.fixture(scope="function")
def mm_test_session_objects(
    model_1,
    emission_1,
    emission_2,
    run_11,
    run_12,
    data_file_1,
    data_file_2,
    data_file_3,
    dfv_dsg_time_series_11,
    dfv_dsg_time_series_12,
    dfv_dsg_time_series_21,
    dfv_dsg_time_series_31,
    ensemble_1,
    ensemble_2,
    ensemble_dfvs_1,
    ensemble_dfvs_2,
):
    # Note: Order matters. These objects must be inserted in the order given,
    # and removed in the reverse order.
    # Note: Completeness matters. SQLAlchemy will implicitly add objects but
    # not necessarily implicitly delete them.
    return (
        [
            model_1,
            emission_1,
            emission_2,
            run_11,
            run_12,
            data_file_1,
            data_file_2,
            data_file_3,
            dfv_dsg_time_series_11,
            dfv_dsg_time_series_12,
            dfv_dsg_time_series_21,
            dfv_dsg_time_series_31,
            ensemble_1,
            ensemble_2,
        ] +
        ensemble_dfvs_1 +
        ensemble_dfvs_2
    )


@pytest.fixture(scope="function")
def mm_test_session(mm_empty_session, mm_test_session_objects):
    s = mm_empty_session
    for obj in mm_test_session_objects:
        s.add(obj)
        s.flush()
    yield s


# TODO: Consider substituting delete actions for rollback everywhere
@pytest.fixture(scope="function")
def mm_test_session_committed(mm_test_session, mm_test_session_objects):
    # Contents of an uncommitted session can only be seen by that session;
    # i.e., a session is implicitly in a transaction. This is good.
    # Some components of pdp_util, e.g., EnsembleCatalog, creates an
    # independent engine and session to access the database, so we must commit
    # our session contents.
    # However, committing a session leaves gunk in the database that can
    # mess up the database setup for other tests. Hence we have to clean up
    # after ourselves. And commit that cleanup.

    s = mm_test_session
    s.commit()
    yield s
    for obj in reversed(mm_test_session_objects):
        s.delete(obj)
        s.flush()
    s.commit()


# WSGI apps

@pytest.fixture(scope="function")
def ensemble_member_lister(mm_database_dsn):
    return EnsembleMemberLister(mm_database_dsn)


@pytest.fixture(scope="function")
def raster_metadata(mm_database_dsn):
    return RasterMetadata(mm_database_dsn)


# Helper functions as fixtures

@pytest.fixture(scope="session")
def query_params():
    """Returns a query parameter string formed from name-value pairs."""
    def f(nv_pairs):
        return '?' + '&'.join(
            '{}={}'.format(name, value)
            for name, value in nv_pairs if value is not None
        )
    return f


@pytest.fixture(scope="session")
def test_wsgi_app():
    """Generic WSGI app test
    Note: It's OK to name a fixture with test_
    """
    def f(app, url, status, content_type, keys):
        req = Request.blank(url)
        resp = req.get_response(app)

        assert resp.status == status
        if status != '200 OK':
            return resp

        assert resp.content_type == content_type
        if content_type != 'application/json':
            return resp

        body = json.loads(resp.body)
        if keys is not None:
            assert set(body.keys()) == keys
        return body
    return f
