# News / Release Notes

## 1.2.1

*Release Date: 2021-Jun-14*

Release for integration test with `pdp`. 

- Fixes DataFileVariable queries for modelmeta >=0.3.0
- Fixes Python CI build


## 1.2.0

*Release Date: 2021-May-27*

*NOTE: this release is a Python 2 release (hopefully the last) with
several bugfixes backported from the main development branch*

- Fixes a bug in the code that adds multiple files to the /agg
  response's zip file with the same name.
- Adds a filter to the PCDS backend that ensures only stations from a
  network with a truthy "publish" flag will be returned.
- Adds support for single POLYGON filters in the PCDS portal [GH Issue
  32](https://github.com/pacificclimate/pdp_util/issues/32)

## 1.1.0

*Release Date: 2021-Jan-14*

- [Handle empty station list in `get_all_metadata_index_responders()`](https://github.com/pacificclimate/pdp_util/pull/25)
- [Add filepath to /metadata response](https://github.com/pacificclimate/pdp_util/pull/23)
- [Add actions](https://github.com/pacificclimate/pdp_util/pull/18)
- [Migrate to modelmeta 0.3.0](https://github.com/pacificclimate/pdp_util/pull/16)
- [Add parameters to /data/pcds/agg backend endpoint to allow arbitrary lists of selection criteria](https://github.com/pacificclimate/pdp_util/pull/11)

## Pre - 1.1.0

There are no release notes for versions prior to 1.1.0. TBD.
