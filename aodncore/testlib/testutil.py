import os
import tempfile
import uuid
import zipfile

from netCDF4 import Dataset
from six import iteritems
from six.moves.urllib.parse import urlunsplit

from ..pipeline.configlib import LazyConfigManager
from ..pipeline.storage import BaseStorageBroker
from ..testlib.dummyhandler import DummyHandler

try:
    from unittest import mock
except ImportError:
    import mock

__all__ = [
    'NullStorageBroker',
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


class NullStorageBroker(BaseStorageBroker):
    def __init__(self, prefix, fail=False):
        mock_logger = mock.MagicMock()
        super(NullStorageBroker, self).__init__(None, mock_logger)
        self.prefix = prefix
        self.fail = fail

        self.upload_call_count = 0
        self.delete_call_count = 0

    def _delete_file(self, pipeline_file, dest_path_attr):
        if self.fail:
            raise Exception('deliberate failure requested')

    def _post_run_hook(self):
        pass

    def _pre_run_hook(self):
        pass

    def _upload_file(self, pipeline_file, dest_path_attr):
        if self.fail:
            raise Exception('deliberate failure requested')

    def _get_absolute_dest_uri(self, pipeline_file, dest_path_attr):
        return "null://{dest_path}".format(dest_path=pipeline_file.dest_path)

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return not self.fail

    def upload(self, pipeline_files, is_stored_attr, dest_path_attr):
        self.upload_call_count += 1
        super(NullStorageBroker, self).upload(pipeline_files, is_stored_attr, dest_path_attr)

    def delete(self, pipeline_files, is_stored_attr, dest_path_attr):
        self.delete_call_count += 1
        super(NullStorageBroker, self).delete(pipeline_files, is_stored_attr, dest_path_attr)

    def assert_upload_call_count(self, count):
        if self.upload_call_count != count:
            raise AssertionError("upload method call count: {call_count}".format(call_count=self.upload_call_count))

    def assert_upload_called(self):
        if self.upload_call_count == 0:
            raise AssertionError("upload method not called")

    def assert_upload_not_called(self):
        self.assert_upload_call_count(0)

    def assert_delete_call_count(self, count):
        if self.delete_call_count != count:
            raise AssertionError("delete method call count: {call_count}".format(call_count=self.delete_call_count))

    def assert_delete_called(self):
        if self.delete_call_count == 0:
            raise AssertionError("delete method not called")

    def assert_delete_not_called(self):
        self.assert_delete_call_count(0)


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

        For example this::

            make_test_file(testfile,
                           {'title': 'test file', 'site_code': 'NRSMAI'},
                           TEMP = {'standard_name': 'sea_water_temperature'},
                           PSAL = {'standard_name': 'sea_water_salinity'}
            )

        will create (in cdl)::

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

    config._discovered_dest_path_functions = {'dest_path_testing': dest_path_testing}
    config._discovered_handlers = {'DummyHandler': DummyHandler}

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
