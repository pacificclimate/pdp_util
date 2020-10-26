import pytest


@pytest.mark.parametrize('ensemble, status, content_type', [
    (0, '200 OK', 'application/json'),
    (1, '200 OK', 'application/json'),
    (2, '404 Not Found', 'application/json'),
    (None, '400 Bad Request', None),
], indirect=['ensemble'])
@pytest.mark.usefixtures('mm_test_session_committed')
def test_ensemble_member_lister(
    ensemble_member_lister,
    test_wsgi_app,
    query_params,
    ensemble,
    status,
    content_type,
):
    url = query_params(('ensemble_name', ensemble and ensemble.name))
    resp, body = \
        test_wsgi_app(ensemble_member_lister, url, status, content_type)
    if status[:3] == '200':
        data_files = {dfv.file for dfv in ensemble.data_file_variables}
        runs = {df.run for df in data_files}
        emissions = {run.emission for run in runs}
        assert set(body.keys()) == {e.short_name for e in emissions}
    if status[:3] == '404':
        assert body['code'] == 404
        assert body['message'] == 'Not Found'
        assert ensemble.name in body['details']
