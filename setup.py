import os

from pip.req import parse_requirements
from setuptools import setup, find_packages

from aodncore.version import __version__

ENTRY_POINTS = {
    'unittest.path_functions': [
        'dest_path_testing = aodncore.testlib:dest_path_testing'
    ]
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
    test_suite='test_aodncore',
    entry_points=ENTRY_POINTS
)
