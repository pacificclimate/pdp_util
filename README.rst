========
pdp_util
========

The `pdp_util` Python package is essentially a package that hosts a variety of utility modules that are required to run the `PCIC Data Portal <http://www.pacificclimate.org/data>`_. In practice, it contains a large part of the non-`Pydap <http://www.pydap.org>`_ code for the PCIC Data Portal [#non-pcic]_, while the `pdp` package pulls everything together in a single application. The utilies can be subdivided into those that support the "pcds" [#pcds]_ portal, those that support the "raster" portals, and those that serve both (i.e. globally applicable utilities).

pcds Utilities
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


.. rubric:: Footnotes

.. [#pcds] Provincial Climate Data Set
.. [#non-pcic] Please note, that I can't imagine this package being useful to anyone other than PCIC (aside from the case where someone wants to understand how we run our application). However, if it does happen to be so, you are welcome to use it under the terms outlined in the LICENSE.txt file.
