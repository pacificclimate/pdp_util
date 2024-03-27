# News / Release Notes

## 2.1.3

*Release Date: 2024-Mar-27*

Changes:
- [Fix method to discriminate climatological variables](https://github.com/pacificclimate/pdp_util/pull/55)

## 2.1.2

*Release Date: 2023-Dec-01*

Changes:
- [Fix PyPI publishing](https://github.com/pacificclimate/pdp_util/pull/54)

## 2.1.1

*Release Date: 2023-Dec-01*

This release marks two things:
- The change of the dependency management system from Pipenv to Poetry. This is an entirely internal development matter.
- Significant changes to the dependencies themselves ([PR #52](https://github.com/pacificclimate/pdp_util/pull/52)). These can affect client packages.

**This release makes no changes to the `pdp_util` API.**

Dependency changes:
- Removes unnecessary explicit dependencies
- Upgrades many dependencies to their latest versions, most importantly `pycds`, `modelmeta`, `pydap-extras`, `sqlalchemy`
- Loosens many dependency version constraints

Additionally:
- Adjusts queries and tests accordingly
- Removes now superfluous Docker test infrastructure
- Moves pytest.ini into pyproject.toml 

Despite the absence of API changes, this is a major move for this package. It is being released partly in order to allow experimentation if necessary with the unchanged API and functionality.

There was a question in my mind about how to increment the version number for
such a change. Semver addresses this [explicitly](https://semver.org/#what-should-i-do-if-i-update-my-own-dependencies-without-changing-the-public-api). In this case, with no new features, it is a patch change. (It is debatable whether it is even that, since it does not to my knowledge fix any bugs, but we'll let that slide.)

## 2.1.0

*Release Date: 2021-Aug-18*

- [Support most recent database schema](https://github.com/pacificclimate/pdp_util/pull/37)
- [Supporting filtering on single polygons in the PCDS portal](https://github.com/pacificclimate/pdp_util/commit/ec4689d05f80df4719e96d91c543dbb0126f492d)
- [Take a networks "publish" flag into account when filtering data in the PCDS portal](https://github.com/pacificclimate/pdp_util/commit/53eca28b59cb9b22205bed02a3299a9fde948032)
- [Fix a bug that allowed multiple copies of the same network file in a downloaded zipfile in the PCDS portal](https://github.com/pacificclimate/pdp_util/commit/7ed607c6ebb09651e521c6fb3419bfa5420b7df1)
- [Integrate Orca for filling some backend requests](https://github.com/pacificclimate/pdp_util/pull/29)

## 2.0.0

*Release Date: 2021-April-27*

- [Update for Python 3](https://github.com/pacificclimate/pdp_util/pull/31)

## 1.1.0

*Release Date: 2021-Jan-14*

- [Handle empty station list in `get_all_metadata_index_responders()`](https://github.com/pacificclimate/pdp_util/pull/25)
- [Add filepath to /metadata response](https://github.com/pacificclimate/pdp_util/pull/23)
- [Add actions](https://github.com/pacificclimate/pdp_util/pull/18)
- [Migrate to modelmeta 0.3.0](https://github.com/pacificclimate/pdp_util/pull/16)
- [Add parameters to /data/pcds/agg backend endpoint to allow arbitrary lists of selection criteria](https://github.com/pacificclimate/pdp_util/pull/11)

## Pre - 1.1.0

There are no release notes for versions prior to 1.1.0. TBD.
