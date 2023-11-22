from os.path import dirname, sep

import pytest
from bs4 import BeautifulSoup

from pdp_util.map import MapApp


@pytest.fixture(scope="session")
def kwargs(pkg_file_root):
    return {
        "app_root": "",
        "js_files": [],
        "css_files": [],
        "templates": str(pkg_file_root("pdp_util") / "templates"),
        "version": "2.0",
        "title": "My test MapApp",
    }


def test_can_instantiate(kwargs):
    map_app = MapApp(**kwargs)
    assert isinstance(map_app, MapApp)


# FIXME: we can't actually _do_ a package-independent render now that the templates are in pdp... *grumble*
def notest_minimal_render(kwargs):
    map_app = MapApp(**kwargs)
    text = map_app({}, lambda x, y: "")

    assert kwargs["title"] in text
    assert kwargs["version"] in text
    soup = BeautifulSoup(text)
    assert kwargs["title"] == soup.head.title.text


def test_missing_required_args(kwargs):
    local_kwargs = kwargs.copy()
    del local_kwargs["js_files"]
    with pytest.raises(ValueError):
        map_app = MapApp(**local_kwargs)
