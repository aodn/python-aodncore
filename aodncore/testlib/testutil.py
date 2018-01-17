import os
import tempfile
import uuid
import zipfile

from netCDF4 import Dataset
from six import iteritems
from six.moves.urllib.parse import urlunsplit

from ..pipeline.configlib import LazyConfigManager
from ..util import discover_entry_points, merge_dicts

try:
    from unittest import mock
except ImportError:
    import mock

__all__ = [
    'dest_path_testing',
    'get_nonexistent_path',
    'make_test_file',
    'make_zip',
    'mock',
    'get_test_config',
    'patch_test_config'
]

GLOBAL_TEST_BASE = os.path.dirname(os.path.dirname(__file__))

TESTLIB_CONF_DIR = os.path.join(os.path.dirname(__file__), 'conf')


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
    test_pipeline_config_file = os.path.join(TESTLIB_CONF_DIR, 'pipeline.conf')
    test_trigger_config_file = os.path.join(TESTLIB_CONF_DIR, 'trigger.conf')
    test_watch_config_file = os.path.join(TESTLIB_CONF_DIR, 'watches.conf')
    os.environ['PIPELINE_CONFIG_FILE'] = test_pipeline_config_file
    os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = test_trigger_config_file
    os.environ['PIPELINE_WATCH_CONFIG_FILE'] = test_watch_config_file

    config = LazyConfigManager()
    patch_test_config(config, GLOBAL_TEST_BASE, temp_dir)

    return config
