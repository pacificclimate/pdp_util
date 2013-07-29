import pytest
from pdp_util.dbdict import DbDict

def test_multiform_key_overwrite():
    d = DbDict()
    # Make sure the key overwrites itself even in different forms
    d['postgresql+psycopg2://hiebert@monsoon.pcic.uvic.ca/crmp'] = 'foo'
    assert len(d.keys()) == 1
    conn_params = {'database': 'crmp', 'user': 'hiebert', 'host': 'monsoon.pcic.uvic.ca'}
    d[conn_params] = 'bar'
    assert len(d.keys()) == 1
    assert d.keys() == ['postgresql+psycopg2://hiebert@monsoon.pcic.uvic.ca/crmp']

def test_password_substitution():
    d = DbDict()
    conn_params = {'database': 'crmp', 'user': 'hiebert', 'password': 'MyTerriblePasswurd', 'host': 'monsoon'}
    d[conn_params] = 'blah'
    assert d['postgresql+psycopg2://hiebert:MyTerriblePasswurd@monsoon/crmp'] == 'blah'

def test_exceptions():
    d = DbDict()
    with pytest.raises(KeyError):
        d[{'database': 'crmp', 'user': 'hiebert'}]

    with pytest.raises(KeyError):
        d[{'database': 'crmp', 'host': 'windy.pcic'}]
        
