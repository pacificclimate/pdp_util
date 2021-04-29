import sys
import string
from setuptools import setup
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ["-v", "--tb=short", "--pdb", "tests"]
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest

        errno = pytest.main(self.test_args)
        sys.exit(errno)


__version__ = (2, 0, 0)

setup(
    name="pdp_util",
    package_dir={"pdp_util": "pdp_util"},
    description="A package supplying numerous apps for running PCIC's data server",
    keywords="sql database opendap dods dap data science climate oceanography meteorology",
    packages=["pdp_util"],
    version=".".join(str(d) for d in __version__),
    url="http://www.pacificclimate.org/",
    author="James Hiebert",
    author_email="hiebert@uvic.ca",
    install_requires=[
        "webob",
        "openid2rp",
        "genshi==0.7.5",
        "paste",
        "beaker",
        "pillow",
        "pytz",
        "simplejson",
        "pycds >=0.0.20",
        "numpy",
        "python-dateutil",
        "pydap==3.2.2",
        "pydap-extras",
        "modelmeta ==0.1.0",
        "PyYAML",
    ],
    tests_require=["pytest", "sqlalchemy", "beautifulsoup4"],
    cmdclass={"test": PyTest},
    zip_safe=True,
    package_data={"pdp_util": ["data/alpha.png", "data/*.css", "templates/*.html"]},
    classifiers="""Development Status :: 5 - Production/Stable
Environment :: Console
Environment :: Web Environment
Intended Audience :: Developers
Intended Audience :: Science/Research
License :: OSI Approved :: GNU General Public License v3 (GPLv3)
Operating System :: OS Independent
Programming Language :: Python :: 3
Topic :: Internet
Topic :: Internet :: WWW/HTTP :: WSGI
Topic :: Scientific/Engineering
Topic :: Software Development :: Libraries :: Python Modules""".split(
        "\n"
    ),
)
