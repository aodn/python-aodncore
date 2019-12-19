from setuptools import setup, find_packages

ENTRY_POINTS = {
    'pipeline.module_versions': [
        'compliance-checker = compliance_checker:__version__'
    ]
}

INSTALL_REQUIRES = [
    'boto3>=1.9.156',
    'celery==4.3.0',
    'compliance-checker==4.1.1',
    'Jinja2==2.10.3',
    'jsonschema>=2.6.0',
    'OWSLib>=0.16.0',
    'paramiko==2.6.0',
    'python-dateutil<2.8.1',  # dateutil capped due to: https://github.com/boto/botocore/issues/1872
    'python-magic>=0.4.15',
    'tabulate==0.8.2',
    'transitions==0.7.1'
]

TESTS_REQUIRE = [
    'httpretty==0.9.7'
]

EXTRAS_REQUIRE = {
    'testing': TESTS_REQUIRE,
    ':platform_system == "Linux"': ['pyinotify == 0.9.6']
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

setup(
    name=PACKAGE_NAME,
    version='1.0.11',
    scripts=PACKAGE_SCRIPTS,
    packages=find_packages(exclude=PACKAGE_EXCLUDES),
    package_data=PACKAGE_DATA,
    url='https://github.com/aodn',
    license='GPLv3',
    author='AODN',
    author_email='developers@emii.org.au',
    description='AODN pipeline library',
    zip_safe=False,
    python_requires='>=3.5',
    install_requires=INSTALL_REQUIRES,
    extras_require=EXTRAS_REQUIRE,
    tests_require=TESTS_REQUIRE,
    test_suite='test_aodncore',
    entry_points=ENTRY_POINTS,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
