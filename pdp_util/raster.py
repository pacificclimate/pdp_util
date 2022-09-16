import os
from os.path import basename

from tempfile import NamedTemporaryFile
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from pdp_util import session_scope
from modelmeta import (
    DataFile,
    DataFileVariableGridded,
    EnsembleDataFileVariables,
    Ensemble,
    VariableAlias,
)

from simplejson import dumps
from webob.request import Request
from webob.response import Response

pwd = os.getcwd()
config = {
    "name": "testing-server",
    "version": 0,
    "api_version": 0,
    "handlers": [
        {
            "url": "/my.nc",
            "file": os.path.join(pwd, "miroc_3.2_20c_A1B_daily_nc3_0_100.nc"),
        },
        {
            "url": "/my.h5",
            "file": os.path.join(
                pwd,
                "pr+tasmax+tasmin_day_BCCA+ANUSPLIN300+CanESM2_historical+rcp26_r1i1p1_19500101-21001231.h5",
            ),
        },
        {
            "url": "/stuff/",
            "dir": "/home/data/climate/downscale/CMIP5/anusplin_downscaling_cmip5/downscaling_outputs/",
        },
    ],
}

JSON_headers = [("Content-type", "application/json; charset=utf-8")]


def response_200(start_response, body):
    start_response("200 OK", JSON_headers)
    return dumps(body)


def response_404(start_response, details):
    start_response("404 Not Found", JSON_headers)
    return dumps({"code": 404, "message": "Not Found", "details": details})


class RasterServer(object):
    """Does dynamic (non-filebased) configuration, for serving rasters"""

    def __init__(self, dsn, config=config):
        """Initialize the application

        :param config: A config dict that can be read by :py:func:`yaml.load` and includes the key `handlers`. `handlers` must be a list of dicts each containing the keys: `url` and `file`.
        :type config: dict
        """
        self._config = config
        self.dsn = dsn

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):
        """Makes catalog requests, but defers to OPeNDAP Request
        Compiler Application (ORCA) for data requests"""

        print("RasterServer.__call__ made")
        print(environ)
        print("end environ")
        req = Request(environ)

        if req.path_info == "/catalog.json":
            with session_scope(self.dsn) as sesh:
                urls = db_raster_catalog(
                    sesh, self.config["ensemble"], self.config["root_url"]
                )
            res = Response(
                body=dumps(urls, indent=4),
                content_type="application/json",
                charset="utf-8",
            )
            return res(environ, start_response)
        elif req.path_info.split(".")[-1] == "das":
            url = build_das_url(self.config["handlers"], self.config["orca_root"], req)
        elif req.path_info.split(".")[-1] in ["dds", "ascii"]:
            url = build_dds_ascii_url(
                self.config["handlers"], self.config["orca_root"], req
            )
        else:
            url = build_orca_url(self.config["handlers"], self.config["orca_root"], req)
        print("generated url {}".format(url))
        return Response(status_code=301, location=url)


class RasterCatalog(RasterServer):
    """WSGI app which is a subclass of RasterServer.  Filters the urls on call to permit only MetaData requests"""

    def __call__(self, environ, start_response):
        """An override of RasterServer's __call__ which allows only MetaData requests"""
        print("in RasterCatalog.__call__")
        req = Request(environ)
        if req.path_info in ["/", "/catalog.json"]:
            environ["PATH_INFO"] = "/catalog.json"
            return super(RasterCatalog, self).__call__(environ, start_response)
        elif req.path_info.split(".")[-1] in ["das", "dds"]:
            print("das or dds call made")
            return super(RasterCatalog, self).__call__(environ, start_response)
        else:
            return response_404(
                start_response, f"URL path '{str(req.path_info)}' not found"
            )


class EnsembleCatalog(object):
    """WSGI app to list an ensemble catalog"""

    def __init__(self, dsn, config=config):
        self._config = config
        self.dsn = dsn

    @property
    def config(self):
        return self._config

    def __call__(self, environ, start_response):
        with session_scope(self.dsn) as sesh:
            urls = db_raster_catalog(
                sesh, self.config["ensemble"], self.config["root_url"]
            )
        res = Response(
            body=dumps(urls, indent=4), content_type="application/json", charset="utf-8"
        )
        return res(environ, start_response)


class RasterMetadata(object):
    """WSGI app to query metadata from the MDDB."""

    def __init__(self, dsn):
        """Initialize the application

        :param dsn: sqlalchemy-style dns string with database dialect and connection options. Example: "postgresql://scott:tiger@localhost/test"
        :type config: dict
        """

        def session_scope_factory():
            return session_scope(dsn)

        self.session_scope_factory = session_scope_factory

    def __call__(self, environ, start_response):
        """Handle requests for metadata"""

        req = Request(environ)

        # Get and validate required parameters
        try:
            unique_id = req.params["id"]
        except:
            start_response("400 Bad Request", [])
            return ["Required parameter 'id' not specified"]

        try:
            var = req.params["var"]
        except:
            start_response("400 Bad Request", [])
            return ["Required parameter 'var' not specified"]

        # Establish content of response

        # Default content
        content_items = {"min", "max"}

        # Content specified by 'include' query param
        try:
            include_items = set(req.params["include"].split(","))
            if not (include_items <= {"filepath", "units"}):
                start_response("400 Bad Request", [])
                return ["Invalid value(s) in 'include' parameter"]
            content_items |= include_items
        except KeyError:
            pass

        # Content specified by 'request' query param
        try:
            request_qp = req.params["request"]
            if request_qp not in {"GetMinMax", "GetMinMaxWithUnits"}:
                start_response("400 Bad Request", [])
                return ["Invalid value for 'request' parameter"]
            if request_qp == "GetMinMaxWithUnits":
                content_items |= {"units"}
        except KeyError:
            pass

        # Build query according to content of response

        # Default content
        columns = (
            DataFileVariableGridded.range_min.label("min"),
            DataFileVariableGridded.range_max.label("max"),
        )
        joins = (DataFile,)

        # 'filepath' content
        if "filepath" in content_items:
            columns += (DataFile.filename.label("filepath"),)

        # 'units' content
        if "units" in content_items:
            columns += (VariableAlias.units.label("units"),)
            joins += (VariableAlias,)

        with self.session_scope_factory() as sesh:
            q = (
                sesh.query(*columns)
                .filter(DataFile.unique_id == unique_id)
                .filter(DataFileVariableGridded.netcdf_variable_name == var)
            )
            for Table in joins:
                q = q.join(Table)

        # Execute query
        try:
            result = q.one()
        except NoResultFound:
            return response_404(
                start_response, "Unable to find specified combination of id and var"
            )
        except MultipleResultsFound:
            return response_404(
                start_response,
                "Multiple matches to specified id and var combination. "
                "This should not happen.",
            )

        # Build and return response
        content = {key: getattr(result, key) for key in content_items}
        print("content: {}".format(content))
        return response_200(start_response, content)


def build_orca_url(handlers, orca_root, req):
    """orca is the OPeNDAP Request Compiler Application which pulls apart large OPeNDAP requests
    to THREDDS into bite-sized chunks and then reasemmbles them for the user.

    Orca is available through a url with one of the formats:
    1. [orca_root]/?filepath=[filepath]
    2. [orca_root]/?filepath=[filepath]&targets=time[time_start:time_end],lat[lat_start:lat_end],lon[lon_start:lon_end],[variable][time_start:time_end][lat_start:lat_end][lon_start:lon_end]

    where the [filepath] can be attained by the mapping of handler url to handler file from a config dict
    """
    print("in build_orca_url")
    print("req is")
    print(req.__dict__)
    print("end req")
    filename = None
    for handler in handlers:
        # print("handler url: {} path_info: {}".format(handler["url"], req.path_info[:-3]))
        if handler["url"] == req.path_info[:-3]:
            print("filename match default")
            filename = handler["file"]
            break
        elif handler["url"] == req.path_info[:-3].strip("/."):
            print("filename match stripped")
            filename = handler["file"].strip("/,")
            break

    print("req.query_string is {}".format(req.query_string))
    if req.query_string == "":
        return f"{orca_root}/?filepath={filename}"
    else:
        dims = get_target_dims(req.query_string[:-1])
        return f"{orca_root}/?filepath={filename}&targets={dims}{req.query_string[:-1]}&outfile={req.path_info.strip('/.')}"


def get_target_dims(var):
    """Adds the dimensions with the bounds matching the data variable to the targets
    for orca data requests."""
    bounds = var[var.index("[") :]
    split_bounds = [bound + "]" for bound in bounds.split("]")][:-1]
    target_dims = ""
    for dim, bnd in zip(["time", "lat", "lon"], split_bounds):
        if bnd == "[]":  # Get entire range of dimension
            target_dims += dim + ","
        else:
            target_dims += dim + bnd + ","
    return target_dims


def build_das_url(handlers, orca_root, req):
    """Builds the URL for a DAS request. The URL has the form
    [orca_root][filepath].das."""
    filepath = get_filepath_from_handlers(
        handlers, remove_final_extension(req.path_info)
    )
    return f"{orca_root}/?filepath={filepath}.das&outfile={req.path_info.strip('/.')}"


def build_dds_ascii_url(handlers, orca_root, req):
    """Builds the URL for a DDS/ASCII request. The URL has the form
    [orca_root][filepath].[dds,ascii]?[variable]"""
    final_extension = req.path_info.split(".")[-1]
    filepath = get_filepath_from_handlers(
        handlers, remove_final_extension(req.path_info)
    )
    return f"{orca_root}/?filepath={filepath}.{final_extension}&targets={req.query_string}&outfile={req.path_info.strip('/.')}"


def remove_final_extension(filename):
    """Given a string containing one or more ., remove the last . and
    everything after it. useful for parsing pyDAP or ORCA URLs, where
    you typically have a filename, followed by a . and then the format you
    want; test1.nc.das means you're requesting a .das describing test1.nc"""
    return filename[0 : filename.rfind(".")] if "." in filename else filename


def get_filepath_from_handlers(handlers, filename):
    """THREDDS uses a full filepath to access data. Get the full filepath for a
    give filename."""
    for handler in handlers:
        if handler["url"] == filename:
            print("filename match default")
            return handler["file"]
        elif handler["url"] == filename.strip("/."):
            print("filename match stripped")
            return handler["file"].strip("/.")
    print("filepath not found: {}".format(filename))


def db_raster_catalog(session, ensemble, root_url):
    """A function which queries the database for all of the raster files belonging to a given ensemble. Returns a dict where keys are the dataset unique ids and the value is the filename for the dataset.

    :param session: SQLAlchemy session for the pcic_meta database
    :param ensemble: Name of the ensemble for which member files should be listed
    :param root_url: Base URL which should be prepended to the beginning of each dataset ID
    :rtype: dict
    """
    files = ensemble_files(session, ensemble)
    return {
        id.replace("+", "-"): root_url + basename(filename)
        for id, filename in files.items()
    }  # FIXME: remove the replace when ncwms stops being dumb


def db_raster_configurator(session, name, version, api_version, ensemble, root_url="/"):
    """A function to construct a config dict which is usable for configuring Pydap for serving rasters

    :param session: SQLAlchemy session for the pcic_meta database
    :param name: Name of this server e.g. `my-raster-server`
    :param version: Version of the server application
    :param api_version: OPeNDAP API version?
    :param ensemble: The identifier for the PCIC MetaData DataBase (:class:`Mddb`) ensemble to configure
    :param root_url: URL to prepend to all of the dataset ids
    """
    files = ensemble_files(session, ensemble)
    config = {
        "name": name,
        "version": version,
        "api_version": api_version,
        "ensemble": ensemble,
        "root_url": root_url,
        "handlers": [
            {"url": basename(filename), "file": filename}
            for id, filename in files.items()
        ],
    }
    return config


def ensemble_files(session, ensemble_name):
    q = (
        session.query(DataFile)
        .join(DataFileVariableGridded)
        .join(EnsembleDataFileVariables)
        .join(Ensemble)
        .filter(Ensemble.name == ensemble_name)
    )
    return {row.unique_id: row.filename for row in q}
