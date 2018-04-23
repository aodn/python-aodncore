import os
from uuid import uuid4

from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import FileDeleteFailedError, FileUploadFailedError, InvalidStoreUrlError
from aodncore.pipeline.steps.store import StoreRunner, get_store_runner
from aodncore.pipeline.storage import LocalFileStorageBroker, S3StorageBroker, SftpStorageBroker
from aodncore.testlib import BaseTestCase, NullStorageBroker
from test_aodncore import TESTDATA_DIR

GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
INVALID_PNG = os.path.join(TESTDATA_DIR, 'invalid.png')
TEST_ICO = os.path.join(TESTDATA_DIR, 'test.ico')
UNKNOWN_FILE_TYPE = os.path.join(TESTDATA_DIR, 'test.unknown_file_extension')


def get_upload_collection(delete=False):
    publish_type = PipelineFilePublishType.DELETE_ONLY if delete else PipelineFilePublishType.UPLOAD_ONLY

    netcdf_file = PipelineFile(GOOD_NC, is_deletion=delete)
    netcdf_file.publish_type = publish_type
    netcdf_file.dest_path = 'subdirectory/targetfile.nc'

    png_file = PipelineFile(INVALID_PNG, is_deletion=delete)
    png_file.publish_type = publish_type
    png_file.dest_path = 'subdirectory/targetfile.png'

    js_file = PipelineFile(TEST_ICO, is_deletion=delete)
    js_file.publish_type = publish_type
    js_file.dest_path = 'subdirectory/targetfile.ico'

    unknown_file = PipelineFile(UNKNOWN_FILE_TYPE, is_deletion=delete)
    unknown_file.publish_type = publish_type
    unknown_file.dest_path = 'subdirectory/targetfile.unknown_file_extension'

    collection = PipelineFileCollection([netcdf_file, png_file, js_file, unknown_file])
    return collection


def get_undo_collection():
    pipeline_file = PipelineFile(GOOD_NC)
    pipeline_file.should_undo = True
    pipeline_file.is_stored = True
    pipeline_file.publish_type = PipelineFilePublishType.UPLOAD_ONLY
    pipeline_file.dest_path = 'subdirectory/targetfile.nc'
    collection = PipelineFileCollection([pipeline_file])
    return collection


# noinspection PyUnusedLocal
class TestPipelineStepsStore(BaseTestCase):
    def test_get_store_runner(self):
        file_uri = 'file:///tmp/probably/doesnt/exist/upload'
        file_store_runner = get_store_runner(file_uri, None, self.test_logger)
        self.assertIsInstance(file_store_runner.broker, LocalFileStorageBroker)

        s3_uri = "s3://{dummy_bucket}/{dummy_prefix}".format(dummy_bucket=str(uuid4()), dummy_prefix=str(uuid4()))
        s3_store_runner = get_store_runner(s3_uri, None, self.test_logger)
        self.assertIsInstance(s3_store_runner.broker, S3StorageBroker)

        sftp_uri = "sftp://{dummy_host}/{dummy_path}".format(dummy_host=str(uuid4()), dummy_path=str(uuid4()))
        sftp_store_runner = get_store_runner(sftp_uri, None, self.test_logger)
        self.assertIsInstance(sftp_store_runner.broker, SftpStorageBroker)

        with self.assertRaises(InvalidStoreUrlError):
            _ = get_store_runner('invalid_uri', None, self.test_logger)


class TestBaseStoreRunner(BaseTestCase):
    def test_delete_fail(self):
        collection = get_upload_collection(delete=True)
        runner = StoreRunner(NullStorageBroker("/", fail=True), None, None)
        with self.assertRaises(FileDeleteFailedError):
            runner.run(collection)

        self.assertFalse(collection[0].is_stored)

    def test_delete_success(self):
        collection = get_upload_collection(delete=True)
        runner = StoreRunner(NullStorageBroker("/"), None, None)
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)

    def test_upload_fail(self):
        collection = get_upload_collection()
        runner = StoreRunner(NullStorageBroker("/", fail=True), None, None)
        with self.assertRaises(FileUploadFailedError):
            runner.run(collection)

    def test_upload_success(self):
        collection = get_upload_collection()
        runner = StoreRunner(NullStorageBroker("/"), None, None)
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)

    def test_set_is_overwrite_with_no_action_file(self):
        collection = get_upload_collection()

        temp_file = PipelineFile(self.temp_nc_file)
        self.assertIsNone(temp_file.dest_path)
        self.assertIs(temp_file.publish_type, PipelineFilePublishType.NO_ACTION)
        collection.add(temp_file)

        runner = StoreRunner(NullStorageBroker("/"), None, None)
        try:
            runner.set_is_overwrite(collection)
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))
