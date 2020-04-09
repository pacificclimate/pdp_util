from os.path import basename
from pdp_util.raster import ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
import json

import pytest
from webob.request import Request


def test_ensemble1_files(mm_test_session, ensemble1, ensemble1_data_files):
    result = ensemble_files(mm_test_session, ensemble1.name)
    assert result == {df.unique_id: df.filename for df in ensemble1_data_files}


def test_ensemble2_files(mm_test_session, ensemble2, ensemble2_data_files):
    result = ensemble_files(mm_test_session, ensemble2.name)
    assert result == {df.unique_id: df.filename for df in ensemble2_data_files}


@pytest.mark.parametrize('root_url', ['foo/', 'bar/'])
def test_db_raster_catalog(
    mm_test_session, ensemble1, ensemble1_data_files, root_url
):
    result = db_raster_catalog(mm_test_session, ensemble1.name, root_url)
    assert result == {
        df.unique_id: '{}{}'.format(root_url, basename(df.filename))
        for df in ensemble1_data_files
    }


@pytest.mark.parametrize(
    'name, version, api_version, ensemble, root_url, handlers',
    [
        ('A name', 'Version 0.0.0.1', '0.0', 'ensemble1', 'http://root.ca/',
         [
             {'url': 'data_file_1.nc', 'file': '/storage/data_file_1.nc'},
             {'url': 'data_file_2.nc', 'file': '/storage/data_file_2.nc'},
         ]),
])
def test_db_raster_configurator(
    mm_test_session, name, version, api_version, ensemble, root_url, handlers,
):
    result = db_raster_configurator(
        mm_test_session, name, version, api_version, ensemble, root_url,
    )
    assert all(
        x in result.items()
        for x in {
            'root_url': root_url,
            'name': name,
            'version': version,
            'ensemble': ensemble,
            'api_version': api_version,
        }.items()
    )
    assert all(
        x in result['handlers']
        for x in handlers
    )


@pytest.mark.parametrize('url', ['', '/url/is/irrelevant'])
@pytest.mark.parametrize('root_url', [
    'http://root.ca/',
])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_ensemble_catalog(
    mm_database_dsn,
    ensemble1,
    ensemble1_data_files,
    root_url,
    url
):
    config = {
        'ensemble': ensemble1.name,
        'root_url': root_url,
    }
    app = EnsembleCatalog(mm_database_dsn, config)
    req = Request.blank(url)
    resp = req.get_response(app)
    assert resp.status == '200 OK'
    assert resp.content_type == 'application/json'
    body = json.loads(resp.body)
    assert body == {
        df.unique_id: '{}{}'.format(root_url, basename(df.filename))
        for df in ensemble1_data_files
    }


def query_params(nv_pairs):
    return '&'.join(
        '{}={}'.format(name, value)
        for name, value in nv_pairs if value is not None
    )


def do_test_raster_metadata_minmax(
    app,
    req, keys,
    id_, var, status
):
    # This function enables us to parametrize the tests in different ways,
    # but not duplicate test code.
    qps = query_params((('request', req), ('id', id_), ('var', var)))
    req = Request.blank('?{}'.format(qps))
    resp = req.get_response(app)

    assert resp.status == status
    if status != '200 OK':
        return

    assert resp.content_type == 'application/json'
    stats = json.loads(resp.body)
    assert set(stats.keys()) == keys


# Valid request= types, variety of id, var cases.
@pytest.mark.parametrize('req, keys', [
    ('GetMinMax', {'min', 'max'}),
    ('GetMinMaxWithUnits', {'min', 'max', 'units'}),
])
@pytest.mark.parametrize('id_, var, status', [
    ('unique_id_1', 'var_1', '200 OK'),
    ('unique_id_1', 'var_2', '200 OK'),
    ('unique_id_2', 'var_3', '200 OK'),
    ('unique_id_3', 'var_4', '200 OK'),
    ('unique_id_1', 'var_3', '404 Not Found'),
    ('unique_id_1', 'wrong', '404 Not Found'),
    ('wrong', 'var_4', '404 Not Found'),
    (None, 'var_1', '400 Bad Request'),
    ('unique_id_1', None, '400 Bad Request'),
    (None, None, '400 Bad Request'),
])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_raster_metadata_minmax(
    raster_metadata,
    req, keys,
    id_, var, status
):
    do_test_raster_metadata_minmax(
        raster_metadata,
        req, keys,
        id_, var, status
    )


# One final test that doesn't fit the parametrization pattern above.
@pytest.mark.usefixtures('mm_test_session_committed')
def test_raster_metadata_minmax_no_id(raster_metadata):
    do_test_raster_metadata_minmax(
        raster_metadata,
        'INVALID_REQUEST_TYPE', {},
        'unique_id_1', 'var_1',
        '400 Bad Request'
    )
