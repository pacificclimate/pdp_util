import os
from datetime import datetime
from random import random
from tempfile import NamedTemporaryFile
from zipfile import ZipFile

from webob.request import Request

import pdp_util
from pdp_util.agg import (
    PcdsZipApp,
    ziperator,
    get_pcds_responders,
    metadata_index_responder,
    get_all_metadata_index_responders,
)

# import pydap.handlers.pcic

import pytest

stns = [("ARDA", "115084"), ("EC_raw", "1046332"), ("FLNRO-WMB", "369")]

# def test_can_instantiate(test_session, conn_params):
#     app = PcdsZipApp(conn_params, test_session)


def test_metadata_index_responder(test_session):
    response_iter = metadata_index_responder(test_session, "FLNRO-WMB", False)
    response_text = b"".join([line for line in response_iter])

    lines = [
        "variables.variable, variables.standard_name, variables.cell_method, variables.unit",
        '"precipitation", "lwe_thickness_of_precipitation_amount", "time: sum", "mm"',
        '"temperature", "air_temperature", "time: point", "celsius"',
        '"relative_humidity", "relative_humidity", "time: mean", "%"',
        '"wind_speed", "wind_speed", "time: mean", "m s-1"',
        '"wind_direction", "wind_from_direction", "time: mean", "degree"',
    ]
    for line in lines:
        assert line in str(response_text)


@pytest.mark.parametrize(
    "stations, expected_filenames",
    [
        ([], set()),
        (
            stns,
            set(
                [
                    "ARDA/variables.csv",
                    "EC_raw/variables.csv",
                    "FLNRO-WMB/variables.csv",
                ]
            ),
        ),
    ],
)
def test_get_all_metadata_index_responders(
    test_session,
    monkeypatch,
    stations,
    expected_filenames,
):
    # Content is tested elsewhere. Fake it out.
    def fake_metadata_index_responder(sesh, net, climo):
        return []

    monkeypatch.setattr(
        pdp_util.agg, "metadata_index_responder", fake_metadata_index_responder
    )

    resp = get_all_metadata_index_responders(test_session, stations, False)
    filenames = set([filename for filename, content in resp])
    assert filenames == expected_filenames


# def test_get_pcds_responders(conn_params, monkeypatch):
#     # Don't care about content (tested elsewhere); just return the environ
#     monkeypatch.setattr(pydap.handlers.pcic.PcicSqlHandler, '__call__', lambda x, y, z: y)

#     now = datetime.now()
#     response = get_pcds_responders(conn_params, stns, 'csv', (now, now), {})

#     expected_names = ['ARDA/115084.csv', 'EC_raw/1046332.csv', 'FLNRO-WMB/369.csv']
#     expected_paths = ['/ARDA/115084.rsql.csv', '/EC_raw/1046332.rsql.csv', '/FLNRO-WMB/369.rsql.csv']

#     for expected_name, expected_path, (name, env) in zip(expected_names, expected_paths, response):
#         assert expected_name == name
#         assert expected_path == env['PATH_INFO']
#         assert str(now) in env['QUERY_STRING']


def test_ziperator():
    random_content = lambda: [str(random()) + "\n" for x in range(10)]
    files = {
        "file0.txt": random_content(),
        "file1.txt": random_content(),
        "file2.txt": random_content(),
    }

    def content_generator(file):
        for line in files[file]:
            yield line

    responders = [(filename, content_generator(filename)) for filename in files.keys()]
    result = ziperator(responders)

    assert hasattr(result, "__iter__")

    with NamedTemporaryFile("wb", delete=False) as f:
        for line in result:
            f.write(line)
        f.flush()
        with ZipFile(f.name) as z:
            # Check that it's a valid archive
            assert z.testzip() is None
            # Check that the names list
            assert set(z.namelist()) == set(files.keys())
            for filename, content in files.items():
                # Check the content of each archive member
                with z.open(filename, "r") as x:
                    assert x.read() == b"".join([line.encode() for line in content])

        os.remove(f.name)
