========
pdp_util
========

.. image:: https://github.com/pacificclimate/pdp_util/workflows/Python%20CI/badge.svg
   :target: https://github.com/pacificclimate/pdp_util

.. image:: https://github.com/pacificclimate/pdp_util/workflows/Pypi%20Publishing/badge.svg
   :target: https://github.com/pacificclimate/pdp_util


Python package `pdp_util` provides a variety of utility modules that are required to run the `PCIC Data Portal <http://www.pacificclimate.org/data>`_. In practice, it contains a large part of the non-`Pydap <http://www.pydap.org>`_ code for the PCIC Data Portal [#non-pcic]_, while the `pdp` package pulls everything together in a single application. The utilies can be subdivided into those that support the "pcds" [#pcds]_ portal, those that support the "raster" portals, and those that serve both (i.e. globally applicable utilities).

PCDS Portal Utilities
--------------

* The `agg` module provides aggregation utilities to translate a single HTTP request into multiple OPeNDAP requests, returning a single response.
* The `counts` module provides two WSGI applications for providing counts/estimates of the number of stations/observations (respectively) in a group of PCDS stations.
* The `filters` module provides error checking and validation for HTTP POST variables describing PCDS station filters.
* The `legend` module provides a WSGI application that creates colored legend symbols for PCDS stations.
* The `pcds_dispatch` module provides a WSGI application which simulates a directory tree and dispatches requests to various PyDAP-based applications. It provides the basis for our data listings pages.
* The `pcds_index` module provides several applications that return various parts of the station listings directory tree.
* The `util` module provides a few functions for parsing and validating HTTP POST variables.

Raster Portal Utilities
-----------------------

The `ensemble_members` and `raster` modules both provide glue applications for communicating with PCIC's metadata database of climate model output (all of which is multi-dimensional "raster" data, otherwise described as being "spatiotemporal" data).

Globally applicable utilities
-----------------------------

* The `map` module provides a WSGI application which outputs the data selection map along with a variety of user interface controls. It is primarily configured through JavaScript includes.
* The `auth` module contains WSGI middleware for providing authentication via OpenID.
* The `dbdict` module contains a function to convert dict object into a database DSN.

Local installation for development
-----------------------------

If your workstation has GDAL 3, postgresql, and python 3 and you are on the PCIC VPN, this package can be installed in a virtual environment with `Poetry <https://python-poetry.org/>`_ and configured to connect to the replicated database at db3.pcic.uvic.ca by setting the `DSN` and `PCDS_DSN` environment variables (passwords are available in Team Password Manager) accordingly. This provides a fairly low-overhead way to run tests.

Installation complications
~~~~

This package currently depends on `pydap-extras 1.0.0 <https://github.com/pacificclimate/pydap-extras>`_, which in turn depends on GDAL 3.0.4.

GDAL 3.0.4 is tricky to install correctly. The following rigmarole, specifically with pre-installations and a special version of `setuptools`, appears to the only way to get a successful installation. A brief explanation follows:

#. GDAL 3.0.4 requires something called `use_2to3`. Modern versions of `setuptools` do not support it; only versions `setuptools<58` do. See, for example,

   *   https://github.com/nextgis/pygdal/issues/67
   *   https://github.com/pypa/setuptools/issues/2781
   *   https://github.com/OSGeo/gdal/issues/7541

   We must therefore explicitly install `setuptools<58` before we install `gdal`.

#. GDAL 3.0.4 cannot be installed successfully by later versions of `pip`. Version 22.3.1 does work. We must ensure it is installed before installing `gdal`.

#. GDAL 3.0.4 depends on `numpy`. This is apparently not declared as a dependency but _is_ flagged by `gdal` as a warning if it is not already installed, and causes the installation to fail. The version must be `numpy<=1.21`. Pre-installing `numpy` solves this problem.

#. Poetry somehow still stumbles over installing `gdal==3.0.4` using its own tooling. However, `gdal` can be installed via Poetry into the virtualenv by using the appropriate version of `pip` (see previous item). This circumvents whatever Poetry does.

#. Once the above steps have been taken, the installation can be completed using the normal `poetry install`.

#. Note that dependencies have been organized into groups to make this as simple as possible. If and when later versions of GDAL are specified, this organization and the installation steps are likely to need to be updated. (Possibly, it will become simpler.)

Hence::

  # Pre-install initial packages (pip, setuptools, numpy)
  poetry install --only initial
  # Install gdal using pip3 into the Poetry virtualenv
  poetry run pip3 install gdal==3.0.4
  # Install rest of project
  poetry install

Additional information
~~~~

Some useful Poetry commands:

* `poetry install` - installs the package and sets up a virtual environment
* `poetry run pytest` - runs pytest, or any other desired command, in the virtual environment in which the package has been installed
* `poetry show --tree` - see a dependency tree
* `poetry shell` - load the virtual environment in the current terminal
* `exit` - exit the Poetry shell. Don't use `deactivate` as you would for a regular python environment, it does not completely exit the Poetry virtualenv.

Additional Poetry documentation is available `here <https://python-poetry.org/docs/>`_.

.. rubric:: Footnotes

.. [#pcds] Provincial Climate Data Set
.. [#non-pcic] Please note, that I can't imagine this package being useful to anyone other than PCIC (aside from the case where someone wants to understand how we run our application). However, if it does happen to be so, you are welcome to use it under the terms outlined in the LICENSE.txt file.

