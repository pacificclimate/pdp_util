import re
from collections import namedtuple
from datetime import datetime

from webob.request import Request
from sqlalchemy import or_

from pycds import CrmpNetworkGeoserver as cng

class FormFilter(namedtuple('FormFilter', 'input_name regex sql_constraint')):
    ''' A simple class for validating form input and mapping the input to a database constraint on the crmp_network_geoserver table
    '''
    def __call__(self, value):
        '''A FormFilter object can be called with form input value.  If the input matches the filters regular expression (i.e. it is valid)
           the call will return a string which is an SQL constraint on the crmp_network_geoserver table

           :param value: an input value to be tested against this FormFilter
           :type value: str
           :rtype: str or None
        '''
        m = re.match(self.regex, value)
        if m:
            return self.sql_constraint(value) if callable(self.sql_constraint) else self.sql_constraint % value
        else:
            return None

    def validate(self, value):
        m = re.match(self.regex, value)
        return value if m else None

    def __str__(self):
        return '<FormFilter> %s:%s' % (self.name, self.value)


def mk_mp_regex():
    decimal = r'-?[0-9]+(.[0-9]+)?'
    point = '%(decimal)s %(decimal)s' % locals()
    inner = r'\(%(point)s(, ?%(point)s){2,}\)' % locals()
    outer = inner
    polygon = r'\(%(outer)s(, ?%(inner)s)?\)' % locals()
    multipolygon = r'MULTIPOLYGON ?\(%(polygon)s(, ?%(polygon)s)*\)' % locals()
    return multipolygon

form_filters = {
    'from-date': FormFilter(
        # Single valid date in format '%Y/%m/%d'
        'from-date',
        r'[0-9]{4}/[0-9]{2}/[0-9]{2}',
        lambda x: cng.max_obs_time > datetime.strptime(x, '%Y/%m/%d')
    ),

    'to-date': FormFilter(
        # Single valid date in format '%Y/%m/%d'
        'to-date',
        r'[0-9]{4}/[0-9]{2}/[0-9]{2}',
        lambda x: cng.min_obs_time < datetime.strptime(x, '%Y/%m/%d')
    ),

    'network-name': FormFilter(
        # Comma-separated list (as string) of network names
        'network-name',
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r'[A-Za-z_,]*',
        # Warning: The following filter makes the empty string (empty list)
        # match *all* networks. This preserves backward compatibility with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: len(x) == 0 or cng.network_name.in_(x.split(','))
    ),

    'input-var': FormFilter(
        # Single variable identifier
        'input-var',
        r'[a-z: _]+',
        lambda x: cng.vars.like('%{}%'.format(x))
    ),

    'input-vars': FormFilter(
        # Comma-separated list (as string) of variable identifiers
        'input-vars',
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r'[a-z: _,]*',
        # The following filter checks whether any variable name `v` in the
        # comma-separated list `x` of variable names occurs in `cng.vars`
        # (which is itself a comma-separated list of variable names).
        #
        # Warning: This filter makes the empty string (empty list)
        # match *all* variables. This makes it consistent with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: len(x) == 0 or or_(
            *map(lambda v: cng.vars.like('%{}%'.format(v)), x.split(','))
        )
    ),

    'input-freq': FormFilter(
        # Comma-separated list (as string) of valid frequency identifiers
        'input-freq',
        # The following simple regex for a comma-separated list does not exclude
        # empty list items (two successive commas) ... but it is simple.
        r'(1-hourly|irregular|daily|12-hourly|,)*',
        # Warning: The following filter makes the empty string (empty list)
        # match *all* frequencies. This preserves backward compatibility with
        # the previous (non-list) API but is counterintuitive for a list-
        # oriented API.
        lambda x: len(x) == 0 or cng.freq.in_(x.split(','))
    ),

    'input-polygon': FormFilter(
        'input-polygon',
        mk_mp_regex(),
        "ST_intersects(ST_GeomFromText('%s', 4326), the_geom)"
    ),

    'only-with-climatology': FormFilter(
        'only-with-climatology',
        'only-with-climatology',
        lambda x: or_(cng.vars.like('%within%'), cng.vars.like('%over%'))
    ),
}

def validate_vars(environ):
    '''Iterate over the POST variables and convert them to SQL constraints
    
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
    '''
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
