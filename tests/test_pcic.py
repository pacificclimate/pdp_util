from pdp_util import get_session

def test_dsn_saving():
    cp = {'database': 'crmp', 'user': 'hiebert', 'host': 'monsoon.pcic'}
    s1 = get_session(cp)
    s2 = get_session(cp)
    assert s1 == s2
    assert s1() != s2()

