import os

from pip.req import parse_requirements
from setuptools import setup, find_packages

from aodncore.version import __version__

ENTRY_POINTS = {
    'pipeline.module_versions': [
        'compliance-checker = compliance_checker:__version__',
        'cc-plugin-imos = cc_plugin_imos:__version__'
    ]
}

EXTRAS_REQUIRE = {
    ':platform_system == "Linux"': ['pyinotify == 0.9.6'],
    ':python_version < "3.3"': ['mock == 2.0.0'],
    ':python_version < "3.5"': ['scandir == 1.6'],
}

PACKAGE_DATA = {
    'aodncore': [
        'pipeline/templates/*.j2',
        'testlib/conf/*.conf'
    ]
}

PACKAGE_EXCLUDES = ['test_aodncore.*', 'test_aodncore']
PACKAGE_NAME = 'aodncore'
PACKAGE_SCRIPTS = ['aodncore/bin/drawmachine.py', 'aodncore/pipeline/watchservice.py']

requirements_txt = os.path.join(os.path.dirname(__file__), 'requirements.txt')
requires = [str(r.req) for r in parse_requirements(requirements_txt, session=False)]

setup(
    name=PACKAGE_NAME,
    version=__version__,
    scripts=PACKAGE_SCRIPTS,
    packages=find_packages(exclude=PACKAGE_EXCLUDES),
    package_data=PACKAGE_DATA,
    url='https://github.com/aodn',
    license='GPLv3',
    author='AODN',
    author_email='developers@emii.org.au',
    description='AODN pipeline library',
    zip_safe=False,
    install_requires=requires,
    extras_require=EXTRAS_REQUIRE,
    test_suite='test_aodncore',
    entry_points=ENTRY_POINTS
)
