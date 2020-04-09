import pytest


@pytest.mark.parametrize('ensemble, status, content_type, keys', [
    ('ensemble_1', '200 OK', 'application/json', {'emission_1', 'emission_2'}),
    ('non_existent', '200 OK', 'text/plain', None),
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
    url = query_params((('ensemble_name', ensemble),))
    resp = test_wsgi_app(ensemble_member_lister, url, status, content_type, keys)
    if ensemble == 'non_existent':
        assert resp.body == ''
