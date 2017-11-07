import ast
import inspect
import os
import sys
import tempfile
import uuid
import zipfile
from distutils.core import run_setup

import pkg_resources
from netCDF4 import Dataset
from pkg_resources import WorkingSet
from six import iteritems
from six.moves.urllib.parse import urlunsplit

from aodncore.pipeline import HandlerBase
from aodncore.pipeline.config import LazyConfigManager
from aodncore.util import discover_entry_points, merge_dicts, CaptureStdIO

try:
    from unittest import mock
except ImportError:
    import mock

__all__ = [
    'dest_path_testing',
    'get_nonexistent_path',
    'make_zip',
    'mock',
    'MOCK_LOGGER',
    'get_entry_points_from_paths',
    'get_test_config',
    'get_test_working_set',
    'patch_test_config',
    'probe_root_package_path',
    'regenerate_egg_info',
    'regenerate_metadata'
]

GLOBAL_TEST_BASE = os.path.dirname(os.path.dirname(__file__))

MOCK_LOGGER = mock.MagicMock()


def dest_path_testing(filename):
    """Example/test function for destination path resolution

    :param filename:
    :return: relative path prepended with DUMMY
    """
    return os.path.join('DUMMY', os.path.relpath(filename, '/'))


def get_nonexistent_path(relative=False):
    """Return a path that is guaranteed not to exist

    :return: string containing guaranteed non-existent path
    """

    path = os.path.join("nonexistent/path/with/a/{uuid}/in/the/middle".format(uuid=uuid.uuid4()))
    if not relative:
        path = os.path.join('/', path)
    assert not os.path.exists(path)
    return path


def make_test_file(filename, attributes=None, **variables):
    """Create a netcdf file with the given global and variable
    attributes. Variables are created as dimensionless doubles.

    For example this:

        make_test_file(testfile,
                       {'title': 'test file', 'site_code': 'NRSMAI'},
                       TEMP = {'standard_name': 'sea_water_temperature'},
                       PSAL = {'standard_name': 'sea_water_salinity'}
        )

    will create (in cdl):

        netcdf testfile {
        variables:
            double PSAL ;
                    PSAL:standard_name = "sea_water_salinity" ;
            double TEMP ;
                    TEMP:standard_name = "sea_water_temperature" ;

        // global attributes:
                    :site_code = "NRSMAI" ;
                    :title = "test file" ;
        }

    """
    if attributes is None:
        attributes = {}

    with Dataset(filename, 'w') as ds:
        ds.setncatts(attributes)
        for name, adict in iteritems(variables):
            var = ds.createVariable(name, float)
            var.setncatts(adict)


# TODO: make this a util function in core module
def make_zip(temp_dir, file_list):
    """Create a zip file in tmp_dir containing the files listed in file_list.
    Return the full path to the zip file.
    """
    _, zip_file = tempfile.mkstemp(dir=temp_dir, suffix='.zip')

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in file_list:
            zf.write(file_path, os.path.basename(file_path))

    return zip_file


def patch_test_config(config, rel_path, temp_dir):
    """Update config object with dynamic runtime values (e.g. temp directories)

    :param config: config dictionary to update
    :param rel_path:
    :param temp_dir: temporary directory used to update values
    :return: ConfigParser object patched with runtime test values
    """
    temp_upload_dir = tempfile.mkdtemp(prefix='temp_upload_uri', dir=temp_dir)

    config.pipeline_config['global']['error_dir'] = tempfile.mkdtemp(prefix='temp_error_dir', dir=temp_dir)
    config.pipeline_config['global']['processing_dir'] = tempfile.mkdtemp(prefix='temp_processing_dir', dir=temp_dir)
    config.pipeline_config['global']['tmp_dir'] = temp_dir
    config.pipeline_config['global']['upload_uri'] = urlunsplit(('file', None, temp_upload_dir, None, None))
    config.pipeline_config['global']['wip_dir'] = rel_path
    config.pipeline_config['logging']['log_root'] = tempfile.mkdtemp(prefix='temp_log_root', dir=temp_dir)
    config.pipeline_config['mail']['smtp_server'] = str(uuid.uuid4())

    # re-run discovery of entry points, and include test entry points. This is to support the two testing scenarios of:
    # 1) tests in aodncore needing to operate on 'unittest.*' namespaces, since we don't want the package to define
    #    entry points under the *real* group name
    # 2) tests in extension modules (i.e. aodndata) need to discover both the real entry points they themselves define,
    #    in addition to the test entry points in aodncore for when they are not specifically testing the objects
    #    represented by the real entry points
    test_path_functions = discover_entry_points('unittest.path_functions')
    test_handlers = discover_entry_points('unittest.handlers')
    real_path_functions = discover_entry_points(config.pipeline_config['pluggable']['path_function_group'])
    real_handlers = discover_entry_points(config.pipeline_config['pluggable']['handlers_group'])
    config._discovered_dest_path_functions = merge_dicts(real_path_functions, test_path_functions)
    config._discovered_handlers = merge_dicts(real_handlers, test_handlers)

    return config


def get_test_config(temp_dir):
    test_pipeline_config_file = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'pipeline.conf')
    test_trigger_config_file = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'trigger.conf')
    test_watch_config_file = os.path.join(GLOBAL_TEST_BASE, 'pipeline', 'watches.conf')
    os.environ['PIPELINE_CONFIG_FILE'] = test_pipeline_config_file
    os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = test_trigger_config_file
    os.environ['PIPELINE_WATCH_CONFIG_FILE'] = test_watch_config_file

    config = LazyConfigManager()
    patch_test_config(config, GLOBAL_TEST_BASE, temp_dir)

    return config


def get_test_working_set(*package_paths):
    """Get a WorkingSet populated with packages,

    :param package_paths: one or more paths to the package(s) to be added to the WorkingSet
    :return: the populated WorkingSet instance
    """
    regenerate_egg_info(*package_paths)
    ws = WorkingSet(package_paths)
    return ws


def get_entry_points_from_paths(entry_point_name, *package_paths):
    """Discover entry points advertised under the given entry point group name, in the given path(s).
        Additionally add any entry points with the same suffix under the unittest namespace (e.g. discovering
        'pipeline.handlers' will also automatically discover objects in the 'unittest.handlers' group

    :param entry_point_name: entry point group name
    :param package_paths: one or more paths to use for discovery
    :return: dict containing discovered entry points
    """
    ws = get_test_working_set(*package_paths)
    discovered_entry_points = discover_entry_points(entry_point_name, ws)

    entry_point_type = entry_point_name.split('.')[-1]
    unittest_entry_points = discover_entry_points('unittest.{t}'.format(t=entry_point_type), ws)
    all_entry_points = merge_dicts(discovered_entry_points, unittest_entry_points)

    return all_entry_points


def probe_root_package_path(object_):
    """Given an object, attempt to return the *root* package path
    e.g.
    Given an instance of 'aodncore.pipeline.HandlerBase', attempt to return the filesystem path to 'aodncore'

    :param object_: object to derive root package from
    :return: path to the package root
    """
    module_name = inspect.getmodule(object_).__name__
    parent_module_name = module_name.split('.')[0]
    package_path = os.path.dirname(os.path.dirname(sys.modules[parent_module_name].__file__))
    setup_script = os.path.join(package_path, 'setup.py')
    assert os.path.exists(setup_script)
    package_name = None
    with file(setup_script) as f:
        for line in f:
            if line.startswith('PACKAGE_NAME'):
                package_name = ast.parse(line).body[0].value.s
                break
    if package_name is None:
        raise EnvironmentError('unable to find package name')
    return package_name, package_path


def regenerate_egg_info(*package_paths):
    """Regenerate 'egg_info' metadata for a

    :param package_paths: one or more package paths in which to generate 'egg_info' metadata
    :return: None
    """
    old_cwd = os.getcwd()
    for package_path in package_paths:
        setup_py = os.path.join(package_path, 'setup.py')
        os.chdir(package_path)
        try:
            with CaptureStdIO() as (_, _):
                run_setup(setup_py, ['egg_info'])
        finally:
            os.chdir(old_cwd)


def regenerate_metadata(handler_class):
    """Regenerate the egg-info metadata in the package containing the given handler class. This is to enable entry
        point discovery in a package directory for unit testing.

    :param handler_class: instance of a HandlerBase sub-class
    :return: None
    """
    package_tuple = probe_root_package_path(handler_class)
    packages_to_regenerate = [package_tuple]

    if package_tuple[0] != 'aodncore':
        core_package_tuple = probe_root_package_path(HandlerBase)
        packages_to_regenerate.insert(0, core_package_tuple)

    for package_name, package_path in packages_to_regenerate:
        print("REGENERATING METADATA FOR package '{}' IN '{}'".format(package_name, package_path))
        regenerate_egg_info(package_path)
        pkg_resources.get_distribution(package_name).activate()
