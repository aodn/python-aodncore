from setuptools import setup, find_packages

ENTRY_POINTS = {
    'pipeline.module_versions': [
        'compliance-checker = compliance_checker:__version__'
    ]
}

EXTRAS_REQUIRE = {
    ':platform_system == "Linux"': ['pyinotify == 0.9.6'],
    ':python_version < "3.3"': ['mock == 2.0.0'],
    ':python_version < "3.4"': ['enum34==1.1.6'],
    ':python_version < "3.5"': ['scandir == 1.9.0', 'typing==3.6.6'],
}

INSTALL_REQUIRES = [
    'boto3==1.9.27',
    'celery==4.2.1',
    'compliance-checker==4.1.1',
    'Jinja2==2.10',
    'jsonschema==2.6.0',
    'numpy>=1.15.2',
    'paramiko==2.4.2',
    'six==1.11.0',
    'tabulate==0.8.2',
    'transitions==0.6.8'
]

PACKAGE_DATA = {
    'aodncore': [
        'pipeline/templates/*.j2',
        'testlib/conf/*.conf'
    ]
}

PACKAGE_EXCLUDES = ['test_aodncore.*', 'test_aodncore']
PACKAGE_NAME = 'aodncore'
PACKAGE_SCRIPTS = ['aodncore/bin/drawmachine.py', 'aodncore/pipeline/watchservice.py']

version = {}
with open('aodncore/version.py') as f:
    exec(f.read(), version)

setup(
    name=PACKAGE_NAME,
    version=version['__version__'],
    scripts=PACKAGE_SCRIPTS,
    packages=find_packages(exclude=PACKAGE_EXCLUDES),
    package_data=PACKAGE_DATA,
    url='https://github.com/aodn',
    license='GPLv3',
    author='AODN',
    author_email='developers@emii.org.au',
    description='AODN pipeline library',
    zip_safe=False,
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    test_suite='test_aodncore',
    entry_points=ENTRY_POINTS
)
