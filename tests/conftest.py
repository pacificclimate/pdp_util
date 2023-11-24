import datetime
import importlib
import logging
import json
from collections import OrderedDict

import alembic
import alembic.config
import alembic.command
from pycds import CrmpNetworkGeoserver, VarsPerHistory
from webob.request import Request

import pytest

import pycds
import pycds.alembic
import modelmeta
from modelmeta import (
    Model,
    Emission,
    Run,
    Grid,
    DataFile,
    DataFileVariableGridded,
    VariableAlias,
    Ensemble,
)

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
import testing.postgresql

from pdp_util.ensemble_members import EnsembleMemberLister
from pdp_util.raster import RasterMetadata


@pytest.fixture(scope="session")
def pkg_file_root():
    def f(package):
        """
        Returns a Path object that is the root of the package's "file system".
        Additional path elements can be appended with the "/" operator.
        """
        with importlib.resources.path(package, "") as root:
            return root

    return f


@pytest.fixture(scope="session")
def schema_name():
    return pycds.get_schema_name()


@pytest.fixture(scope="session")
def database_uri():
    """URI of test PG database"""
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope="session")
def base_engine(database_uri):
    """Plain vanilla database engine, with nothing added."""
    yield create_engine(database_uri)


def initialize_database(engine, schema_name):
    """Initialize an empty database"""
    # Add role required by PyCDS migrations for privileged operations.
    engine.execute(f"CREATE ROLE {pycds.get_su_role_name()} WITH SUPERUSER NOINHERIT;")
    # Add extensions required by PyCDS.
    engine.execute("CREATE EXTENSION postgis")
    engine.execute("CREATE EXTENSION plpython3u")
    engine.execute("CREATE EXTENSION IF NOT EXISTS citext")
    # Add schema.
    engine.execute(CreateSchema(schema_name))


@pytest.fixture(scope="session")
def pycds_engine(base_engine, database_uri, schema_name):
    initialize_database(base_engine, schema_name)
    yield base_engine


@pytest.fixture(scope="session")
def alembic_script_location():
    """
    This fixture extracts the filepath to the installed pycds Alembic content.
    The filepath is typically like
    `/usr/local/lib/python3.6/dist-packages/pycds/alembic`.
    """
    try:
        import importlib_resources

        source = importlib_resources.files(pycds.alembic)
    except ModuleNotFoundError:
        import importlib.resources

        if hasattr(importlib.resources, "files"):
            source = importlib.resources.files(pycds.alembic)
        else:
            with importlib.resources.path("pycds", "alembic") as path:
                source = path

    yield str(source)


def migrate_database(script_location, database_uri, revision="head"):
    """
    Migrate a database to a specified revision using Alembic.
    This requires a privileged role to be added in advance to the database.
    """
    alembic_config = alembic.config.Config()
    alembic_config.set_main_option("script_location", script_location)
    alembic_config.set_main_option("sqlalchemy.url", database_uri)
    alembic.command.upgrade(alembic_config, revision)


@pytest.fixture(scope="session")
def empty_session(pycds_engine, alembic_script_location, database_uri):
    migrate_database(alembic_script_location, database_uri)
    Session = sessionmaker(bind=pycds_engine)
    with Session() as session:
        yield session


@pytest.fixture(scope="session")
def collapsed_vars_mv_sql():
    """
    SQL to populate table CollapsedVariables / collapsed_vars_mv with test data.

    CrmpNetworkGeoserver depends on the contents of CollapsedVariables.
    CollapsedVariables maps to a manual matview, which must be populated manually.
    The needed data was defined in pycds v2.2.1 crmp_subset_data, in particular,
    embodied in the inserts to crmp_network_geoserver in that file. Later versions
    of pycds have no need of that data, so it was lost removed. This restores it
    for our purposes.
    """
    return """
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1116, 'lwe_thickness_of_precipitation_amount_sum, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperature_minimum, air_temperaturet: minimum within days t: mean within months t: mean over years, thickness_of_snowfall_amount_sum, air_temperature_maximum, thickness_of_rainfall_amount_sum, surface_snow_thickness_point, air_temperaturet: mean within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years', 'Precipitation Amount|Temperature Climatology (Max.)|Temperature (Min.)|Temperature Climatology (Min.)|Snowfall Amount|Temperature (Max.)|Rainfall Amount|Surface Snow Depth (Point)|Temperature Climatology (Mean)|Precipitation Climatology');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(3416, 'lwe_thickness_of_precipitation_sum, air_temperature_minimum, surface_snow_thickness_point, wind_speed_maximum, lwe_thickness_of_precipitation_amount_sum, wind_speed_point, relative_humidity_mean, air_temperature_point, wind_from_direction_mean, thickness_of_snowfall_amount_sum, dew_point_temperature_mean, wind_from_direction_point, lwe_thickness_of_precipitation_amount_sum, air_temperature_point, wind_from_direction_standard_deviation, air_temperaturet: minimum within days t: mean within months t: mean over years, wind_speed_mean, air_temperature_maximum, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperaturet: mean within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_pressure_point', 'Precipitation (Cumulative)|Rainfall Amount|Temperature (Min.)|Surface Snow Depth (Point)|Wind Speed (Max.)|Precipitation Amount|Wind Speed (Point)|Relative Humidity (Mean)|Temperature (Point)|Wind Direction (Mean)|Snowfall Amount|Dew Point Temperature (Mean)|Wind Direction (Point)|Precipitation Amount|Temperature (Point)|Wind Direction (Std Dev)|Temperature Climatology (Min.)|Wind Speed (Mean)|Temperature (Max.)|Temperature Climatology (Max.)|Temperature Climatology (Mean)|Precipitation Climatology|Air Pressure (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(3616, 'surface_snow_thickness_point, lwe_thickness_of_precipitation_amount_sum, air_temperature_point', 'Surface Snow Depth (Point)|Precipitation Amount|Temperature (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(8516, 'air_temperature_minimum, lwe_thickness_of_precipitation_amount_sum, air_temperature_maximum', 'Temperature (Min.)|Precipitation Amount|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1516, 'thickness_of_rainfall_amount_sum, air_temperaturet: mean within days t: mean within months t: mean over years, air_temperaturet: minimum within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperaturet: maximum within days t: mean within months t: mean over years, surface_snow_thickness_point, air_temperature_maximum, lwe_thickness_of_precipitation_amount_sum, air_temperature_minimum, thickness_of_snowfall_amount_sum', 'Rainfall Amount|Temperature Climatology (Mean)|Temperature Climatology (Min.)|Precipitation Climatology|Temperature Climatology (Max.)|Surface Snow Depth (Point)|Temperature (Max.)|Precipitation Amount|Temperature (Min.)|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1816, 'air_temperature_maximum, air_temperaturet: mean within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperaturet: minimum within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amount_sum, air_temperature_minimum, thickness_of_rainfall_amount_sum, air_temperaturet: maximum within days t: mean within months t: mean over years, thickness_of_snowfall_amount_sum, surface_snow_thickness_point', 'Temperature (Max.)|Temperature Climatology (Mean)|Precipitation Climatology|Temperature Climatology (Min.)|Precipitation Amount|Temperature (Min.)|Rainfall Amount|Temperature Climatology (Max.)|Snowfall Amount|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(7516, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(8416, 'thickness_of_snowfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, air_temperature_minimum, air_temperature_maximum', 'Snowfall Amount|Precipitation Amount|Temperature (Min.)|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6716, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Precipitation (Cumulative)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5416, 'lwe_thickness_of_precipitation_sum, air_temperature_minimum, surface_snow_thickness_point, air_temperature_maximum', 'Precipitation (Cumulative)|Temperature (Min.)|Surface Snow Depth (Point)|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5816, 'lwe_thickness_of_precipitation_amount_sum', 'Precipitation Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5216, 'air_temperature_minimum, air_temperature_maximum', 'Temperature (Min.)|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6516, 'air_temperature_minimum, air_temperature_maximum', 'Temperature (Min.)|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(2516, 'wind_speed_mean, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, relative_humidity_mean, air_temperature_point, wind_from_direction_mean, lwe_thickness_of_precipitation_amount_sum', 'Wind Speed (Mean)|Precipitation Climatology|Relative Humidity (Mean)|Temperature (Point)|Wind Direction (Mean)|Precipitation Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(2016, 'relative_humidity_mean, wind_from_direction_mean, lwe_thickness_of_precipitation_amount_sum, wind_speed_mean, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperature_point', 'Relative Humidity (Mean)|Wind Direction (Mean)|Precipitation Amount|Wind Speed (Mean)|Precipitation Climatology|Temperature (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5716, 'lwe_thickness_of_precipitation_sum, air_temperature_maximum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point, air_temperature_minimum', 'Precipitation (Cumulative)|Temperature (Max.)|Precipitation (Cumulative)|Surface Snow Depth (Point)|Temperature (Min.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5916, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Precipitation (Cumulative)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1316, 'lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperaturet: mean within days t: mean within months t: mean over years, surface_snow_thickness_point, air_temperature_maximum, thickness_of_snowfall_amount_sum, air_temperaturet: minimum within days t: mean within months t: mean over years, air_temperature_minimum, thickness_of_rainfall_amount_sum, lwe_thickness_of_precipitation_amount_sum', 'Precipitation Climatology|Temperature Climatology (Max.)|Temperature Climatology (Mean)|Surface Snow Depth (Point)|Temperature (Max.)|Snowfall Amount|Temperature Climatology (Min.)|Temperature (Min.)|Rainfall Amount|Precipitation Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6916, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Precipitation (Cumulative)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1216, 'air_temperature_minimum, air_temperaturet: minimum within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperature_maximum, surface_snow_thickness_point, lwe_thickness_of_precipitation_amount_sum, thickness_of_rainfall_amount_sum, thickness_of_snowfall_amount_sum, air_temperaturet: mean within days t: mean within months t: mean over years', 'Temperature (Min.)|Temperature Climatology (Min.)|Precipitation Climatology|Temperature Climatology (Max.)|Temperature (Max.)|Surface Snow Depth (Point)|Precipitation Amount|Rainfall Amount|Snowfall Amount|Temperature Climatology (Mean)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(7716, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Precipitation (Cumulative)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5316, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1416, 'air_temperature_minimum, lwe_thickness_of_precipitation_amount_sum, air_temperature_maximum, surface_snow_thickness_point, thickness_of_rainfall_amount_sum, thickness_of_snowfall_amount_sum', 'Temperature (Min.)|Precipitation Amount|Temperature (Max.)|Surface Snow Depth (Point)|Rainfall Amount|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5616, 'surface_snow_thickness_point, lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Surface Snow Depth (Point)|Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(8216, 'air_temperature_minimum, air_temperature_maximum, lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Temperature (Min.)|Temperature (Max.)|Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1016, 'lwe_thickness_of_precipitation_amount_sum, air_temperaturet: maximum within days t: mean within months t: mean over years, surface_snow_thickness_point, thickness_of_rainfall_amount_sum, air_temperature_minimum, thickness_of_snowfall_amount_sum, air_temperaturet: minimum within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperature_maximum, air_temperaturet: mean within days t: mean within months t: mean over years', 'Precipitation Amount|Temperature Climatology (Max.)|Surface Snow Depth (Point)|Rainfall Amount|Temperature (Min.)|Snowfall Amount|Temperature Climatology (Min.)|Precipitation Climatology|Temperature (Max.)|Temperature Climatology (Mean)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(516, 'lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, thickness_of_rainfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, surface_snow_thickness_point, thickness_of_snowfall_amount_sum', 'Precipitation Climatology|Rainfall Amount|Precipitation Amount|Surface Snow Depth (Point)|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6316, 'lwe_thickness_of_precipitation_sum, surface_snow_thickness_point, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Surface Snow Depth (Point)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(416, 'thickness_of_rainfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, thickness_of_snowfall_amount_sum', 'Rainfall Amount|Precipitation Amount|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(7316, 'lwe_thickness_of_precipitation_sum, surface_snow_thickness_point, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Surface Snow Depth (Point)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6116, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(8016, 'lwe_thickness_of_precipitation_amount_sum, air_temperature_minimum, surface_snow_thickness_point, air_temperature_maximum', 'Precipitation Amount|Temperature (Min.)|Surface Snow Depth (Point)|Temperature (Max.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1616, 'air_temperature_maximum, air_temperaturet: mean within days t: mean within months t: mean over years, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperaturet: minimum within days t: mean within months t: mean over years, thickness_of_snowfall_amount_sum, thickness_of_rainfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperature_minimum', 'Temperature (Max.)|Temperature Climatology (Mean)|Temperature Climatology (Max.)|Temperature Climatology (Min.)|Snowfall Amount|Rainfall Amount|Precipitation Amount|Precipitation Climatology|Temperature (Min.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(816, 'air_temperature_minimum, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperature_maximum, lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, thickness_of_rainfall_amount_sum, air_temperaturet: mean within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amount_sum, air_temperaturet: minimum within days t: mean within months t: mean over years, thickness_of_snowfall_amount_sum', 'Temperature (Min.)|Temperature Climatology (Max.)|Temperature (Max.)|Precipitation Climatology|Rainfall Amount|Temperature Climatology (Mean)|Precipitation Amount|Temperature Climatology (Min.)|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1716, 'lwe_thickness_of_precipitation_amountt: sum within months t: mean over years, air_temperaturet: minimum within days t: mean within months t: mean over years, air_temperature_maximum, air_temperaturet: mean within days t: mean within months t: mean over years, thickness_of_rainfall_amount_sum, surface_snow_thickness_point, air_temperature_minimum, air_temperaturet: maximum within days t: mean within months t: mean over years, lwe_thickness_of_precipitation_amount_sum, thickness_of_snowfall_amount_sum', 'Precipitation Climatology|Temperature Climatology (Min.)|Temperature (Max.)|Temperature Climatology (Mean)|Rainfall Amount|Surface Snow Depth (Point)|Temperature (Min.)|Temperature Climatology (Max.)|Precipitation Amount|Snowfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(716, 'thickness_of_snowfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, surface_snow_thickness_point, thickness_of_rainfall_amount_sum', 'Snowfall Amount|Precipitation Amount|Surface Snow Depth (Point)|Rainfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(3016, 'air_temperature_mean, wind_speed_point, relative_humidity_point, wind_from_direction_point', 'Temperature (Mean)|Wind Speed (Point)|Relative Humidity (Point)|Wind Direction (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6816, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Precipitation (Cumulative)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(2716, 'lwe_thickness_of_precipitation_amount_sum, air_temperature_point, wind_from_direction_mean, wind_speed_mean, relative_humidity_mean', 'Precipitation Amount|Temperature (Point)|Wind Direction (Mean)|Wind Speed (Mean)|Relative Humidity (Mean)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(8316, 'tendency_of_air_pressure_sum, dew_point_temperature_point, wind_speed_point, air_temperature_point, air_temperature_minimum, lwe_thickness_of_precipitation_amount_sum, wind_from_direction_mean, air_temperature_maximum, wind_speed_of_gust_maximum, mean_sea_level_point, thickness_of_snowfall_amount_sum, relative_humidity_point', 'Air Pressure Tendency|Dew Point Temperature (Point)|Wind Speed (Point)|Temperature (Point)|Temperature (Min.)|Precipitation Amount|Wind Direction (Mean)|Temperature (Max.)|Wind Gust (Max.)|Mean Sea Level|Snowfall Amount|Relative Humidity (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(1916, 'lwe_thickness_of_precipitation_amount_sum, air_temperature_maximum, air_temperature_minimum', 'Precipitation Amount|Temperature (Max.)|Temperature (Min.)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(6016, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5116, 'lwe_thickness_of_precipitation_sum, air_temperature_maximum, lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_amount_sum, air_temperature_minimum, surface_snow_thickness_point', 'Precipitation (Cumulative)|Temperature (Max.)|Precipitation (Cumulative)|Precipitation Amount|Temperature (Min.)|Surface Snow Depth (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(3516, 'lwe_thickness_of_precipitation_sum, air_temperature_minimum, surface_snow_thickness_point, wind_speed_maximum, lwe_thickness_of_precipitation_amount_sum, wind_speed_point, relative_humidity_mean, air_temperature_point, wind_from_direction_mean, thickness_of_snowfall_amount_sum, dew_point_temperature_mean, wind_from_direction_point, lwe_thickness_of_precipitation_amount_sum, air_temperature_point, wind_from_direction_standard_deviation, air_temperaturet: minimum within days t: mean within months t: mean over years, wind_speed_mean, air_temperature_maximum, air_temperaturet: maximum within days t: mean within months t: mean over years, air_temperaturet: mean within days t: mean within months t: mean over years, air_pressure_point', 'Precipitation (Cumulative)|Temperature (Min.)|Surface Snow Depth (Point)|Wind Speed (Max.)|Precipitation Amount|Wind Speed (Point)|Relative Humidity (Mean)|Temperature (Point)|Wind Direction (Mean)|Snowfall Amount|Dew Point Temperature (Mean)|Wind Direction (Point)|Precipitation Amount|Temperature (Point)|Wind Direction (Std Dev)|Temperature Climatology (Min.)|Wind Speed (Mean)|Temperature (Max.)|Temperature Climatology (Max.)|Temperature Climatology (Mean)|Air Pressure (Point)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(7816, 'lwe_thickness_of_precipitation_sum, surface_snow_thickness_point, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Surface Snow Depth (Point)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(916, 'air_temperature_maximum, air_temperature_minimum, thickness_of_snowfall_amount_sum, lwe_thickness_of_precipitation_amount_sum, thickness_of_rainfall_amount_sum', 'Temperature (Max.)|Temperature (Min.)|Snowfall Amount|Precipitation Amount|Rainfall Amount');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(7016, 'lwe_thickness_of_precipitation_sum, lwe_thickness_of_precipitation_sum', 'Precipitation (Cumulative)|Precipitation (Cumulative)');
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(4116, NULL, NULL);
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(13216, NULL, NULL);
INSERT INTO collapsed_vars_mv (history_id, vars, display_names) VALUES(5516, NULL, NULL);
"""

# Ideally we could leave this as scope="session" (because it's much
# faster!), but something in the SQL that gets executed below messes
# up the search path such that SQLALchemy can't find any of the
# PostGIS functions. As a demonstration of this behaviour, the
# test_geo.py:test_that_I_can_use_PostGIS function will pass when run
# independently, but will fail if run in the suite where test_session
# is declared with session scope.
@pytest.fixture(scope="function")
def test_session(schema_name, empty_session, pkg_file_root, collapsed_vars_mv_sql):
    logging.getLogger("sqlalchemy.engine").setLevel(logging.ERROR)
    # It's not clear why this has to be set here, given it is set in engine, but ...
    empty_session.execute(f"SET search_path TO {schema_name}, public")

    empty_session.begin_nested()
    # Oh, this is a bad idea
    with open(pkg_file_root("pycds") / "data" / "crmp_subset_data.sql", "r") as f:
        sql = f.read()
    empty_session.execute(sql)
    empty_session.execute(collapsed_vars_mv_sql)
    empty_session.execute(VarsPerHistory.refresh())
    empty_session.commit()

    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO
    )  # Let's not log all the db setup stuff...

    yield empty_session

    empty_session.rollback()
    empty_session.close()


def create_unpublished_network(executor):
    unpub = pycds.Network(id=999, name="MoSecret", publish=False)
    stns = [
        pycds.Station(
            native_id="does not matter",
            network=unpub,
            histories=[
                pycds.History(
                    station_name="Does Not Matter",
                    the_geom="SRID=4326;POINT(-140.866667 62.416667)",
                )
            ],
        )
    ]
    executor.add_all(stns + [unpub])
    executor.commit()


@pytest.fixture(scope="function")
def test_session_with_unpublished(empty_session):
    empty_session.begin_nested()
    create_unpublished_network(empty_session)
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


@pytest.fixture(scope="session")
def mm_database_dsn():
    """Test-session-wide testing.Postgresql instance; returns dsn for it"""
    with testing.postgresql.Postgresql() as pg:
        yield pg.url()


@pytest.fixture(scope="session")
def mm_schema_name():
    return "test_meta"


@pytest.fixture(scope="session")
def mm_engine(mm_database_dsn, mm_schema_name):
    """Test-session-wide database engine"""
    engine = create_engine(mm_database_dsn)
    engine.execute("create extension postgis")
    engine.execute(CreateSchema(mm_schema_name))
    modelmeta.Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def mm_empty_session(mm_engine, mm_schema_name):
    """Single-test database session.
    All session actions are rolled back on teardown"""
    session = sessionmaker(bind=mm_engine)()
    session.execute(f"SET search_path TO {mm_schema_name}, public")
    yield session
    session.rollback()
    session.close()


# Database test object constructors


def make_model(i):
    return Model(short_name=f"model_{i}", type="model_type")


def make_emission(i):
    return Emission(
        short_name=f"emission_{i}",
    )


def make_run(i, model, emission):
    return Run(
        name=f"emission_{i}",
        model=model,
        emission=emission,
    )


def make_data_file(i, run=None, timeset=None):
    return DataFile(
        filename=f"/storage/data_file_{i}.nc",
        first_1mib_md5sum="first_1mib_md5sum",
        unique_id=f"unique_id_{i}",
        x_dim_name="lon",
        y_dim_name="lat",
        t_dim_name="time",
        index_time=datetime.datetime.now(),
        run=run,
        timeset=timeset,
    )


def make_variable_alias(i):
    return VariableAlias(
        long_name=f"long_name_{i}",
        standard_name=f"standard_name_{i}",
        units=f"units_{i}",
    )


def make_grid(i):
    return Grid(
        name="Grid {}".format(i),
        evenly_spaced_y=True,
        xc_count=99,
        xc_grid_step=99,
        xc_origin=99,
        xc_units="furlong",
        yc_count=99,
        yc_grid_step=99,
        yc_origin=99,
        yc_units="furlong",
    )


def make_dfv_gridded(i, file=None, variable_alias=None, grid=None):
    return DataFileVariableGridded(
        derivation_method=f"derivation_method_{i}",
        variable_cell_methods=f"variable_cell_methods_{i}",
        netcdf_variable_name=f"var_{i}",
        disabled=False,
        range_min=0,
        range_max=100,
        file=file,
        variable_alias=variable_alias,
        grid=grid,
    )


def make_ensemble(i, data_file_variables):
    return Ensemble(
        changes="changes",
        description=f"Ensemble {i}",
        name=f"ensemble_{i}",
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


@pytest.fixture(scope="function")
def models():
    return make(make_model, 2)


@pytest.fixture(scope="function")
def emissions():
    return make(make_emission, 2)


@pytest.fixture(scope="function")
def runs(models, emissions):
    return make(
        make_run,
        [
            (models[0], emissions[0]),
            (models[0], emissions[1]),
        ],
    )


@pytest.fixture(scope="function")
def data_files(runs):
    return make(make_data_file, [runs[0], runs[1], runs[0]])


@pytest.fixture(scope="function")
def variable_aliases():
    return make(make_variable_alias, 2)


@pytest.fixture(scope="function")
def grids():
    return make(make_grid, 1)


@pytest.fixture(scope="function")
def dfv_dsg_tss(data_files, variable_aliases, grids):
    grid = grids[0]
    return make(
        make_dfv_gridded,
        [
            (data_files[0], variable_aliases[0], grid),  # var 0, uid 0
            (data_files[0], variable_aliases[1], grid),  # var 1, uid 0
            (data_files[1], variable_aliases[1], grid),  # var 2, uid 1
            (data_files[2], variable_aliases[1], grid),  # var 3, uid 2
        ],
    )


@pytest.fixture(scope="function")
def ensembles(dfv_dsg_tss):
    return make(
        make_ensemble,
        [
            [dfv_dsg_tss[0], dfv_dsg_tss[2]],
            [dfv_dsg_tss[2], dfv_dsg_tss[3]],
            [],
        ],
    )


# Convenience fixtures that retrieve objects from database construction
# fixtures. These fixtures must be called indirectly, and the parameter they
# take is the index of the object in the list returned by the corresponding
# construction fixture; e.g., `ensemble` indexes `ensembles`.
#
# We could do one for each type of object (model, run, etc.), but it turns out
# we only need ensemble. Easy to add other fixtures at need, two lines each.


def get_from(array, index):
    return None if index is None else array[index]


@pytest.fixture(scope="function")
def ensemble(ensembles, request):
    return get_from(ensembles, request.param)


@pytest.fixture(scope="function")
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
        models
        + emissions
        + runs
        + data_files
        + variable_aliases
        + dfv_dsg_tss
        +
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
        return "?" + "&".join(
            f"{name}={value}" for name, value in nv_pairs if value is not None
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

        if content_type != "application/json":
            return resp, None

        resp_body = resp.app_iter[0] if type(resp.app_iter) == list else resp.app_iter
        json_body = json.loads(resp_body)
        return resp, json_body

    return f
