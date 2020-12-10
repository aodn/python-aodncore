import json
import os
import tempfile
import uuid
import zipfile
from collections import OrderedDict
from urllib.parse import urlunsplit

from netCDF4 import Dataset

from ..pipeline.configlib import LazyConfigManager, load_json_file
from ..pipeline.storage import BaseStorageBroker
from ..testlib.dummyhandler import DummyHandler
from ..util import WriteOnceOrderedDict


__all__ = [
    'NullStorageBroker',
    'dest_path_testing',
    'get_nonexistent_path',
    'make_test_file',
    'make_zip',
    'get_test_config',
    'load_runtime_patched_pipeline_config_file'
]

GLOBAL_TEST_BASE = os.path.dirname(os.path.dirname(__file__))

TESTLIB_CONF_DIR = os.path.join(os.path.dirname(__file__), 'conf')
TESTLIB_VOCAB_DIR = os.path.join(os.path.dirname(__file__), 'vocab')


class NullStorageBroker(BaseStorageBroker):
    def __init__(self, prefix, fail=False):
        super().__init__()
        self.prefix = prefix
        self.fail = fail

        self.upload_call_count = 0
        self.delete_call_count = 0
        self.download_call_count = 0
        self.query_call_count = 0

    def _delete_file(self, pipeline_file, dest_path_attr):
        if self.fail:
            raise Exception('deliberate failure requested')

    def _post_run_hook(self):
        pass

    def _pre_run_hook(self):
        pass

    def _download_file(self, remote_pipeline_file):
        if self.fail:
            raise Exception('deliberate failure requested')

    def _upload_file(self, pipeline_file, dest_path_attr):
        if self.fail:
            raise Exception('deliberate failure requested')

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return not self.fail

    def _run_query(self, query):
        if self.fail:
            raise Exception('deliberate failure requested')

    def download(self, remote_pipeline_files, local_path, dest_path_attr='dest_path'):
        self.download_call_count += 1
        super().download(remote_pipeline_files, local_path)

    def upload(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        self.upload_call_count += 1
        super().upload(pipeline_files, is_stored_attr, dest_path_attr)

    def delete(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        self.delete_call_count += 1
        super().delete(pipeline_files, is_stored_attr, dest_path_attr)

    def query(self, query=''):
        self.query_call_count += 1
        super().query(query)

    def assert_download_call_count(self, count):
        if self.download_call_count != count:
            raise AssertionError("download method call count: {call_count}".format(call_count=self.download_call_count))

    def assert_download_called(self):
        if self.download_call_count == 0:
            raise AssertionError("download method not called")

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

    def assert_query_call_count(self, count):
        if self.query_call_count != count:
            raise AssertionError("query method call count: {call_count}".format(call_count=self.delete_call_count))

    def assert_query_called(self):
        if self.query_call_count == 0:
            raise AssertionError("query method not called")

    def assert_query_not_called(self):
        self.assert_query_call_count(0)


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
        for name, adict in variables.items():
            var = ds.createVariable(name, float)
            var.setncatts(adict)


# TODO: make this a util function in core module
def make_zip(temp_dir, file_list):
    """Create a zip file in tmp_dir containing the files listed in file_list.
    Return the full path to the zip file.
    """
    with tempfile.NamedTemporaryFile(dir=temp_dir, suffix='.zip') as f:
        pass
    zip_file = f.name

    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for file_path in file_list:
            zf.write(file_path, os.path.basename(file_path))

    return zip_file


def load_runtime_patched_pipeline_config_file(config_file, rel_path, temp_dir):
    """Load and update pipeline config file with dynamic runtime values (e.g. temp directories)

    :param config_file: Pipeline config file to load, patch and return
    :param rel_path:
    :param temp_dir: temporary directory used to update values
    :return: ConfigParser object patched with runtime test values
    """

    # load the pipeline config file with OrderedDict so that the keys can be patched
    pipeline_config = load_json_file(config_file, object_pairs_hook=OrderedDict)

    temp_upload_dir = tempfile.mkdtemp(prefix='temp_upload_uri', dir=temp_dir)

    pipeline_config['global']['error_uri'] = "file://{}".format(tempfile.mkdtemp(prefix='temp_error_dir', dir=temp_dir))
    pipeline_config['global']['processing_dir'] = tempfile.mkdtemp(prefix='temp_processing_dir', dir=temp_dir)
    pipeline_config['global']['tmp_dir'] = temp_dir
    pipeline_config['global']['upload_uri'] = urlunsplit(('file', None, temp_upload_dir, None, None))
    pipeline_config['global']['wip_dir'] = rel_path
    pipeline_config['logging']['log_root'] = tempfile.mkdtemp(prefix='temp_log_root', dir=temp_dir)
    pipeline_config['mail']['smtp_server'] = str(uuid.uuid4())
    pipeline_config['watch']['incoming_dir'] = tempfile.mkdtemp(prefix='temp_incoming_dir', dir=temp_dir)

    pipeline_config['global']['platform_vocab_url'] = "file://{vocab_dir}/aodn_aodn-platform-vocabulary.rdf".format(
        vocab_dir=TESTLIB_VOCAB_DIR)
    pipeline_config['global'][
        'platform_category_vocab_url'] = "file://{vocab_dir}/aodn_aodn-platform-category-vocabulary.rdf".format(
        vocab_dir=TESTLIB_VOCAB_DIR)
    pipeline_config['global'][
        'xbt_line_vocab_url'] = "file://{vocab_dir}/aodn_aodn-xbt-line-vocabulary.rdf".format(
        vocab_dir=TESTLIB_VOCAB_DIR)

    # reload the JSON object with non-updatable keys
    return json.loads(json.dumps(pipeline_config), object_pairs_hook=WriteOnceOrderedDict)


def get_test_config(temp_dir):
    test_pipeline_config_file = os.path.join(TESTLIB_CONF_DIR, 'pipeline.conf')
    test_trigger_config_file = os.path.join(TESTLIB_CONF_DIR, 'trigger.conf')
    test_watch_config_file = os.path.join(TESTLIB_CONF_DIR, 'watches.conf')

    config = LazyConfigManager()
    config.__dict__['discovered_dest_path_functions'] = ({'dest_path_testing': dest_path_testing}, [])
    config.__dict__['discovered_handlers'] = ({'DummyHandler': DummyHandler}, [])

    config.__dict__['pipeline_config'] = load_runtime_patched_pipeline_config_file(test_pipeline_config_file,
                                                                                   GLOBAL_TEST_BASE, temp_dir)
    config.__dict__['trigger_config'] = load_json_file(test_trigger_config_file)
    config.__dict__['watch_config'] = load_json_file(test_watch_config_file)

    return config
