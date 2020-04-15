import pytest


@pytest.mark.parametrize('ensemble, status, content_type, keys', [
    ('ensemble_1', '200 OK', 'application/json', {'emission_1', 'emission_2'}),
    ('non_existent', '404 Not Found', 'application/json', None),
    (None, '400 Bad Request', None, None),
])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_ensemble_member_lister(
    ensemble_member_lister,
    test_wsgi_app,
    query_params,
    ensemble,
    status,
    content_type,
    keys,
):
    url = query_params(('ensemble_name', ensemble))
    resp, body = \
        test_wsgi_app(ensemble_member_lister, url, status, content_type)
    if keys is not None:
        assert set(body.keys()) == keys
    if status[:3] == '404':
        assert body['code'] == 404
        assert body['message'] == 'Not Found'
        assert ensemble in body['details']
