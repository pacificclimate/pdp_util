import pytest
import pycds
import pdp_util

@pytest.fixture(scope="module")
def test_session():
    sesh = pdp_util.get_session(pycds.test_dsn)()
    return sesh

@pytest.fixture(scope="module")
def conn_params():
    return pycds.test_dsn
