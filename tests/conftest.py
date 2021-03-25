import datetime
import logging
import json
from collections import OrderedDict
from pkg_resources import resource_filename
from webob.request import Request

import pytest

import pycds
import modelmeta
from modelmeta import (
    Model,
    Emission,
    Run,
    DataFile,
    DataFileVariableDSGTimeSeries,
    VariableAlias,
    Ensemble,
)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
import testing.postgresql

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

# Notes:
#
# 1. Some of the subjects under test establish a completely independent
# connection with the database, taking only the database DSN (not a session) as
# an argument. Therefore test contents in the database for such subjects must be
# fully and irrevocably committed to the database. Such a commit *cannot* be
# surrounded by a transaction that is later used to roll back the commit; the
# objects thus committed are only visible within the transaction, and that
# necessarily excludes independent database connections. Therefore we have
# fixtures that truly commit objects to the database. To undo such commits they
# must delete those objects.
#
# 2. SQLAlchemy does not permit adding an object again after it has been
# deleted. An alternative, in fact recommended in the message emitted with the
# error for this attempted action, is to make the object transient again with
# the make_transient() function. Unfortunately, make_transient() causes most
# attributes to be nulled, and the resulting object is useless for our purposes.
# Instead, we find ourselves forced to create new database objects for each
# test setup of the database. The new objects can be inserted again after their
# earlier version (different in identity but identical in content) has been
# deleted.

# TODO: Factor out common engine creation (common with crmp database above)

@pytest.fixture(scope='session')
def mm_database_dsn():
    """Test-session-wide testing.Postgresql instance; returns dsn for it"""
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope='session')
def mm_schema_name():
    return 'test_meta'


@pytest.fixture(scope='session')
def mm_engine(mm_database_dsn, mm_schema_name):
    """Test-session-wide database engine"""
    engine = create_engine(mm_database_dsn)
    engine.execute("create extension postgis")
    engine.execute(CreateSchema(mm_schema_name))
    modelmeta.Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope='function')
def mm_empty_session(mm_engine, mm_schema_name):
    """Single-test database session.
    All session actions are rolled back on teardown"""
    session = sessionmaker(bind=mm_engine)()
    session.execute(
        'SET search_path TO {}, public'.format(mm_schema_name)
    )
    yield session
    session.rollback()
    session.close()


# Database test object constructors

def make_model(i):
    return Model(
        short_name='model_{}'.format(i),
        type='model_type'
    )


def make_emission(i):
    return Emission(
        short_name='emission_{}'.format(i),
    )


def make_run(i, model, emission):
    return Run(
        name='emission_{}'.format(i),
        model=model,
        emission=emission,
    )


def make_data_file(i, run=None, timeset=None):
    return DataFile(
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


def make_variable_alias(i):
    return VariableAlias(
        long_name='long_name_{}'.format(i),
        standard_name='standard_name_{}'.format(i),
        units='units_{}'.format(i),
    )


def make_dfv_dsg_time_series(i, file=None, variable_alias=None):
    return DataFileVariableDSGTimeSeries(
        derivation_method='derivation_method_{}'.format(i),
        variable_cell_methods='variable_cell_methods_{}'.format(i),
        netcdf_variable_name='var_{}'.format(i),
        disabled=False,
        range_min=0,
        range_max=100,
        file=file,
        variable_alias=variable_alias,
    )


def make_ensemble(i, data_file_variables):
    return Ensemble(
        changes='changes',
        description='Ensemble {}'.format(i),
        name='ensemble_{}'.format(i),
        version=float(i),
        data_file_variables=data_file_variables,
    )


def make(maker, arg_list, auto_ids=True):
    """Make a list of database objects using the given maker and args.
    In essence, this function maps `maker` over `arg_list`, spreading the
    tuples in `arg_list` as args for `maker`.
    Some arg munging for the convenience of the user:
    - an "arg_list" equal to an integer means no args for `maker`, but that many items
    - for an arg_list that is a list, non-tuple items are converted to tuples
    """

    def tuplify(x):
        return x if type(x) == tuple else (x,)

    if type(arg_list) == int:
        return [maker(i) for i in range(arg_list)]
    if auto_ids:
        return [maker(i, *tuplify(args)) for i, args in enumerate(arg_list)]
    else:
        return [maker(*tuplify(args)) for args in arg_list]


# Fixtures that create and wire up database objects
# These are all objects that could be inserted into a database, with
# all the appropriate relationships established between them.

@pytest.fixture(scope='function')
def models():
    return make(make_model, 2)


@pytest.fixture(scope='function')
def emissions():
    return make(make_emission, 2)


@pytest.fixture(scope='function')
def runs(models, emissions):
    return make(make_run, [
        (models[0], emissions[0]),
        (models[0], emissions[1]),
    ])


@pytest.fixture(scope='function')
def data_files(runs):
    return make(make_data_file, [runs[0], runs[1], runs[0]])


@pytest.fixture(scope='function')
def variable_aliases():
    return make(make_variable_alias, 2)


@pytest.fixture(scope='function')
def dfv_dsg_tss(data_files, variable_aliases):
    return make(make_dfv_dsg_time_series, [
        (data_files[0], variable_aliases[0]),   # var 0, uid 0
        (data_files[0], variable_aliases[1]),   # var 1, uid 0
        (data_files[1], variable_aliases[1]),   # var 2, uid 1
        (data_files[2], variable_aliases[1]),   # var 3, uid 2
    ])


@pytest.fixture(scope='function')
def ensembles(dfv_dsg_tss):
    return make(make_ensemble, [
        [dfv_dsg_tss[0], dfv_dsg_tss[2]],
        [dfv_dsg_tss[2], dfv_dsg_tss[3]],
        [],
    ])


# Convenience fixtures that retrieve objects from database construction
# fixtures. These fixtures must be called indirectly, and the parameter they
# take is the index of the object in the list returned by the corresponding
# construction fixture; e.g., `ensemble` indexes `ensembles`.
#
# We could do one for each type of object (model, run, etc.), but it turns out
# we only need ensemble. Easy to add other fixtures at need, two lines each.

def get_from(array, index):
    return None if index is None else array[index]


@pytest.fixture(scope='function')
def ensemble(ensembles, request):
    return get_from(ensembles, request.param)


@pytest.fixture(scope='function')
def mm_test_session_objects(
    models,
    emissions,
    runs,
    data_files,
    variable_aliases,
    dfv_dsg_tss,
    ensembles,
):
    """Return a subset of all database objects to be inserted into
    the test session(s).
    """
    return (
        models +
        emissions +
        runs +
        data_files +
        variable_aliases +
        dfv_dsg_tss +
        # Leave out 3rd ensemble so that we have a not-found one
        ensembles[:2]
    )


# Database sessions

@pytest.fixture(scope="function")
def mm_test_session(mm_empty_session, mm_test_session_objects):
    """Session with test objects added. These additions are rolled back
    by mm_empty_session (or deleted by mm_test_session_committed).
    """
    s = mm_empty_session
    s.add_all(mm_test_session_objects)
    s.flush()
    yield s


# TODO: Consider substituting delete actions for rollback everywhere
@pytest.fixture(scope="function")
def mm_test_session_committed(mm_test_session, mm_test_session_objects):
    """Fully committed test database. For an explanation of why, see
    Note 1 above.
    """
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
    """Returns a query parameter string formed from name-value pairs.
    Each pair is an argument; any number may be provided.
    """
    def f(*nv_pairs):
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
    def f(app, url, status, content_type):
        req = Request.blank(url)
        resp = req.get_response(app)

        assert resp.status == status
        assert resp.content_type == content_type

        if content_type != 'application/json':
            return resp, None

        json_body = json.loads(resp.app_iter)
        return resp, json_body
    return f
