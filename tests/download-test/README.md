# Download tests

## Purpose

This directory contains some scripts for running downloads formulated to test modifications
to `pdp_util` in [PR #11](https://github.com/pacificclimate/pdp_util/pull/11). In that PR,
an extension to the API was introduced. These tests were run to verify two things:

- The existing API had not been broken (backwards compatibility maintained).
- The extended API works as expected.

These scripts don't verify anything; that must still be done by hand. Fortunately verification
isn't terribly hard.

The tests are divided into timeseries and climatologies sets, housed in directories of the same
name in this directory. Each such test is described (and evaluated) the `README.md` in each
subdirectory. The timeseries tests are much more comprehensive than the climatology tests.

## Running the tests

Each set of tests runs a couple of PCDS standard (non-extended API) downloads against the production
backend. Then it runs a series of tests, both PCDS standard and SDP extended, against a local
backend assumed to be running at `http://127.0.0.1:8000`.

To start a suitable local backend, do the following:

1. Install `pdp` (in a local venv)
2. Uninstall `pdp-util`: `pip uninstall pdp-util`
3. Install the local version of `pdp_util`: `pip install <local path to pdp_util>`
4. Set up environment variables:
    ```bash
    export CPLUS_INCLUDE_PATH=/usr/include/gdal
    export C_INCLUDE_PATH=/usr/include/gdal
    export DSN=postgresql://httpd_meta:R3@d0nly@db3.pcic.uvic.ca/pcic_meta
    export DATA_ROOT=http://127.0.0.1:8000/data
    export PCDS_DSN=postgresql://httpd:R3@d0nly@db3.pcic.uvic.ca/crmp
    export APP_ROOT=http://127.0.0.1:8000
    ```
5. Mount `/storage`
6. Run PDP locally: `python scripts/rast_serve.py -p 8000`
7. Run the test scripts.

