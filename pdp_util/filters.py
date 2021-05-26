import re
from collections import namedtuple
from datetime import datetime

from webob.request import Request
from sqlalchemy import func, or_

from pycds import CrmpNetworkGeoserver as cng

from sqlalchemy.sql.functions import GenericFunction
from sqlalchemy.types import String
from sqlalchemy.dialects import postgresql
from geoalchemy2.functions import ST_GeomFromText, ST_Intersects

import pdb


def intersects(x):
    geo = ST_GeomFromText(x, 4326)
    return ST_Intersects(geo, cng.the_geom)


# Register the PostgreSQL function `regexp_split_to_array`, with explicit
# typing with postresql.ARRAY. This allows use of PostgreSQL specific operator
# `overlap` (`&&`) in the SQLAlchemy code.
# See https://docs.sqlalchemy.org/en/12/core/type_basics.html#sqlalchemy.types.ARRAY
# Unfortunately this does not solve the problem of comparing expressions
# created with this function. See `tests/test_filters.py`.
class regexp_split_to_array(GenericFunction):
    type = postgresql.ARRAY(String)


class FormFilter(namedtuple("FormFilter", "input_name regex sql_constraint")):
    """A simple class for validating form input and mapping the input to a
    database constraint on the `crmp_network_geoserver` table.

    NOTE regarding filtering on variables:

    Filtering on variables (parameters `input-var` and `input-vars`), is based
    on the column `crmp_network_geoserver.vars`. This column contains a comma-
    separated (actually: ', '-separated) list of variable identifiers aggregated
    over the `history_id`.

    A variable identifier is formed from a row of
    `pycds.Variable` (table `meta_vars`) by concatenating (without separator)
    the column `standard_name` and the string derived from column `cell_method`
    by replacing all occurrences of the string 'time: ' with '_'. It is unknown
    at the date of this writing why this replacement is performed. For
    reference, an identifier is formed by the following PostgreSQL expression
    (see CRMP database view `collapsed_vars_v`, column `vars`):
    ```
    array_to_string(array_agg(meta_vars.standard_name::text ||
    regexp_replace(meta_vars.cell_method::text, 'time: '::text, '_'::text, 'g'::text)),
    ', '::text)
    ```
    """

    def __call__(self, value):
        """A FormFilter object can be called with form input value.  If the input matches the filters regular expression (i.e. it is valid)
        the call will return a string which is an SQL constraint on the crmp_network_geoserver table

        :param value: an input value to be tested against this FormFilter
        :type value: str
        :rtype: str or None
        """
        m = re.match(self.regex, value)
        if m:
            return (
                self.sql_constraint(value)
                if callable(self.sql_constraint)
                else text(self.sql_constraint % value)
            )
        else:
            return None

    def validate(self, value):
        m = re.match(self.regex, value)
        return value if m else None

    def __str__(self):
        return f"<FormFilter> {self.name}:{self.value}"


def mk_mp_regex():
    decimal = r"-?[0-9]+(.[0-9]+)?"
    point = f"{decimal} {decimal}"
    inner = r"\(%(point)s(, ?%(point)s){2,}\)" % locals()
    outer = inner
    single_polygon = r"\(%(outer)s(, ?%(inner)s)?\)" % locals()
    multipolygon = (
        r"MULTIPOLYGON ?\(%(single_polygon)s(, ?%(single_polygon)s)*\)" % locals()
    )
    polygon = r"POLYGON %(single_polygon)s" % locals()
    return r"(%(polygon)s|%(multipolygon)s)" % locals()


form_filters = {
    "from-date": FormFilter(
        # Single valid date in format '%Y/%m/%d'
        "from-date",
        r"[0-9]{4}/[0-9]{2}/[0-9]{2}",
        lambda x: cng.max_obs_time > datetime.strptime(x, "%Y/%m/%d"),
    ),
    "to-date": FormFilter(
        # Single valid date in format '%Y/%m/%d'
        "to-date",
        r"[0-9]{4}/[0-9]{2}/[0-9]{2}",
        lambda x: cng.min_obs_time < datetime.strptime(x, "%Y/%m/%d"),
    ),
    "network-name": FormFilter(
        # Comma-separated list (as string) of network names
        "network-name",
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r"[A-Za-z_,]*",
        # Warning: The following filter makes the empty string (empty list)
        # match *all* networks. This preserves backward compatibility with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: len(x) == 0 or cng.network_name.in_(x.split(",")),
    ),
    "input-var": FormFilter(
        # Single variable identifier
        # See note in class docstring regarding the content of a variable
        # identifier.
        "input-var",
        r"[a-z: _]+",
        lambda x: cng.vars.like(f"%{x}%"),
    ),
    "input-vars": FormFilter(
        # Comma-separated list (as string) of variable identifiers.
        # See note in class docstring regarding the content of a variable
        # identifier.
        "input-vars",
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r"[a-z: _,]*",
        # The following filter checks whether any variable identifier `v` in
        # the comma-separated list `x` of variable identifiers occurs in
        # `cng.vars` (which is itself a comma-separated list of variable
        # identifiers).
        #
        # Unlike parameter `input-var`, an exact match between a variable
        # identifier in `x` and one in `cng.vars` is required. This test uses the
        # PostgreSQL specific function `regexp_split_to_array` and array
        # operator overlap (`&&`) for this test. This code will not necessarily
        # work in any other type of database.
        #
        # Warning: This filter makes the empty string (empty list)
        # match *all* variables. This makes it consistent with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: (
            len(x) == 0
            or func.regexp_split_to_array(cng.vars, ",\\s*").overlap(
                postgresql.array(x.split(","))
            )
        ),
    ),
    "input-freq": FormFilter(
        # Comma-separated list (as string) of valid frequency identifiers
        "input-freq",
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r"(1-hourly|irregular|daily|12-hourly|,)*",
        # Warning: The following filter makes the empty string (empty list)
        # match *all* frequencies. This preserves backward compatibility with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: len(x) == 0 or cng.freq.in_(x.split(",")),
    ),
    "input-polygon": FormFilter("input-polygon", mk_mp_regex(), intersects),
    "only-with-climatology": FormFilter(
        "only-with-climatology",
        "only-with-climatology",
        lambda x: or_(cng.vars.like("%within%"), cng.vars.like("%over%")),
    ),
}


def validate_vars(environ):
    """Iterate over the POST variables and convert them to SQL constraints

    :param environ: dict which can include:

    .. hlist::
       * from-date
       * to-date
       * network-name
       * input-var
       * input-freq
       * input-polygon
       * only-with-climatology

    :rtype: list of callables or text SQL constraints
    """
    req = Request(environ)
    form = req.params

    valid_filters = []
    for k, v in form.items():
        try:
            filt = form_filters[k](v)
            if filt is not None:
                valid_filters.append(filt)
        except KeyError:
            next

    return valid_filters


__all__ = form_filters
