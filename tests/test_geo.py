from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
import testing.postgresql
import pycds


def test_that_I_can_use_PostGIS(empty_session):
    history = pycds.History(station_name='FIVE MILE', the_geom='SRID=4326;POINT(-122.68889 50.91089)')
    empty_session.add(history)
    empty_session.commit()
    return
