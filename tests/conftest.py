import pytest
import pycds
import pdp_util
from pdp_util.ensemble_members import EnsembleMemberLister

@pytest.fixture(scope="module")
def test_session():
    sesh = pdp_util.get_session(pycds.test_dsn)()
    return sesh

@pytest.fixture(scope="module")
def conn_params():
    return pycds.test_dsn

@pytest.fixture(scope="function")
def ensemble_member_lister():
    # FIXME: we need a test DSN
    return EnsembleMemberLister('postgresql://pcic_meta@monsoon.pcic/pcic_meta')
