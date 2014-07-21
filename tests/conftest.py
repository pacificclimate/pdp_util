import pytest
import pycds
import modelmeta
import pdp_util
from pdp_util.ensemble_members import EnsembleMemberLister
from pdp_util.raster import RasterMetadata

@pytest.fixture(scope="module")
def test_session():
    sesh = pdp_util.get_session(pycds.test_dsn)()
    return sesh

@pytest.fixture(scope="module")
def conn_params():
    return pycds.test_dsn

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
