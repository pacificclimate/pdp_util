from os.path import basename
from pdp_util.raster import (
    ensemble_files, db_raster_catalog, db_raster_configurator, EnsembleCatalog
)

import pytest


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
def test_ensemble_files(mm_test_session, ensemble):
    result = ensemble_files(mm_test_session, ensemble.name)
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert result == {df.unique_id: df.filename for df in data_files}


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize('root_url', ['foo/', 'bar/'])
def test_db_raster_catalog(mm_test_session, ensemble, root_url):
    result = db_raster_catalog(mm_test_session, ensemble.name, root_url)
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert result == {
        df.unique_id: '{}{}'.format(root_url, basename(df.filename))
        for df in data_files
    }


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize('name, version, api_version, root_url', [
    ('A name', 'Version 0.0.0.1', '0.0', 'http://example.com/'),
])
def test_db_raster_configurator(
    mm_test_session, name, version, api_version, ensemble, root_url,
):
    result = db_raster_configurator(
        mm_test_session, name, version, api_version, ensemble.name, root_url,
    )

    for x in {
        'root_url': root_url,
        'name': name,
        'version': version,
        'ensemble': ensemble.name,
        'api_version': api_version,
    }.items():
        assert x in result.items()
    data_files = {dfv.file for dfv in ensemble.data_file_variables}

    handlers = [
        {'url': basename(df.filename), 'file': df.filename}
        for df in data_files
    ]
    assert len(result['handlers']) == len(handlers)
    for x in result['handlers']:
        assert x in handlers


@pytest.mark.parametrize("ensemble", [0, 1], indirect=["ensemble"])
@pytest.mark.parametrize('url', ['', '/url/is/irrelevant'])
@pytest.mark.parametrize('root_url', ['http://example.com/'])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_ensemble_catalog(
    mm_database_dsn,
    test_wsgi_app,
    ensemble,
    root_url,
    url
):
    config = {
        'ensemble': ensemble.name,
        'root_url': root_url,
    }
    app = EnsembleCatalog(mm_database_dsn, config)
    resp, body = test_wsgi_app(app, url, '200 OK', 'application/json')
    data_files = {dfv.file for dfv in ensemble.data_file_variables}
    assert body == {
        df.unique_id: '{}{}'.format(root_url, basename(df.filename))
        for df in data_files
    }


# Valid request= types, variety of id, var cases.
@pytest.mark.parametrize('req, keys', [
    ('GetMinMax', {'min', 'max'}),
    ('GetMinMaxWithUnits', {'min', 'max', 'units'}),
])
@pytest.mark.parametrize('id_, var, status, content_type', [
    ('unique_id_0', 'var_0', '200 OK', 'application/json'),
    ('unique_id_0', 'var_1', '200 OK', 'application/json'),
    ('unique_id_1', 'var_2', '200 OK', 'application/json'),
    ('unique_id_2', 'var_3', '200 OK', 'application/json'),
    ('unique_id_1', 'var_0', '404 Not Found', 'application/json'),
    ('unique_id_0', 'wrong', '404 Not Found', 'application/json'),
    ('wrong', 'var_0', '404 Not Found', 'application/json'),
    (None, 'var_0', '400 Bad Request', None),
    ('unique_id_0', None, '400 Bad Request', None),
    (None, None, '400 Bad Request', None),
])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_raster_metadata_minmax(
    raster_metadata,
    test_wsgi_app,
    query_params,
    req, keys,
    id_, var, status, content_type,
):
    url = query_params(('request', req), ('id', id_), ('var', var))
    resp, body = test_wsgi_app(raster_metadata, url, status, content_type)
    if status[:3] == '200':
        assert set(body.keys()) == keys


# One final test that doesn't fit the parametrization pattern above.
@pytest.mark.usefixtures('mm_test_session_committed')
def test_raster_metadata_minmax_no_id(
    raster_metadata, test_wsgi_app, query_params
):
    url = query_params(
        ('request', 'INVALID_REQUEST_TYPE'),
        ('id', 'unique_id_1'),
        ('var', 'var_1')
    )
    test_wsgi_app(raster_metadata, url, '400 Bad Request', None)
