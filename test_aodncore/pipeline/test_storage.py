import errno
import os
from uuid import uuid4

from botocore.exceptions import ClientError

from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import FileDeleteFailedError, FileUploadFailedError, InvalidStoreUrlError
from aodncore.pipeline.storage import (get_storage_broker, sftp_path_exists, sftp_makedirs, sftp_mkdir_p,
                                       LocalFileStorageBroker, S3StorageBroker, SftpStorageBroker)
from aodncore.testlib import BaseTestCase, NullStorageBroker, get_nonexistent_path, mock
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
class TestPipelineStorage(BaseTestCase):
    def test_get_storage_broker(self):
        file_uri = 'file:///tmp/probably/doesnt/exist/upload'
        file_storage_broker = get_storage_broker(file_uri, None, self.test_logger)
        self.assertIsInstance(file_storage_broker, LocalFileStorageBroker)

        s3_uri = "s3://{dummy_bucket}/{dummy_prefix}".format(dummy_bucket=str(uuid4()), dummy_prefix=str(uuid4()))
        s3_storage_broker = get_storage_broker(s3_uri, None, self.test_logger)
        self.assertIsInstance(s3_storage_broker, S3StorageBroker)

        sftp_uri = "sftp://{dummy_host}/{dummy_path}".format(dummy_host=str(uuid4()), dummy_path=str(uuid4()))
        sftp_storage_broker = get_storage_broker(sftp_uri, None, self.test_logger)
        self.assertIsInstance(sftp_storage_broker, SftpStorageBroker)

        with self.assertRaises(InvalidStoreUrlError):
            _ = get_storage_broker('invalid_uri', None, self.test_logger)

    def test_sftp_path_exists_error(self):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        sftpclient.stat.side_effect = IOError()

        with self.assertRaises(IOError):
            _ = sftp_path_exists(sftpclient, path)

    def test_sftp_path_exists_false(self):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        sftpclient.stat.side_effect = IOError(errno.ENOENT, "No such file or directory")

        result = sftp_path_exists(sftpclient, path)
        self.assertFalse(result)

    def test_sftp_path_exists_true(self):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        sftpclient.stat.return_value = True

        result = sftp_path_exists(sftpclient, path)
        self.assertTrue(result)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    def test_sftp_makedirs_parent_dotsegment(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        path_with_dot = os.path.join(path, '.')
        mode = 0o755

        mock_sftp_path_exists.return_value = False

        sftp_makedirs(sftpclient, path_with_dot)

        sftpclient.mkdir.assert_called_with(path, mode)
        self.assertEqual(sftpclient.mkdir.call_count, 9)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    def test_sftp_makedirs_parent_exists(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_path_exists.return_value = True

        sftp_makedirs(sftpclient, path)

        sftpclient.mkdir.assert_called_once_with(path, mode)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    def test_sftp_makedirs_parent_notexists(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_path_exists.return_value = False

        sftp_makedirs(sftpclient, path)

        sftpclient.mkdir.assert_called_with(path, mode)
        self.assertEqual(sftpclient.mkdir.call_count, 9)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    @mock.patch('aodncore.pipeline.storage.sftp_makedirs')
    def test_sftp_mkdir_p_newdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        sftp_mkdir_p(sftpclient, path)

        mock_sftp_makedirs.assert_called_once_with(sftpclient, path, mode)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    @mock.patch('aodncore.pipeline.storage.sftp_makedirs')
    def test_sftp_mkdir_p_ioerror_existingdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_makedirs.side_effect = IOError()
        mock_sftp_path_exists.return_value = True
        sftp_mkdir_p(sftpclient, path)

        mock_sftp_makedirs.assert_called_once_with(sftpclient, path, mode)
        mock_sftp_path_exists.assert_called_once_with(sftpclient, path)

    @mock.patch('aodncore.pipeline.storage.sftp_path_exists')
    @mock.patch('aodncore.pipeline.storage.sftp_makedirs')
    def test_sftp_mkdir_p_error_ioerror_nonexistingdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        mock_sftp_path_exists.return_value = False
        mock_sftp_makedirs.side_effect = IOError()
        with self.assertRaises(IOError):
            sftp_mkdir_p(sftpclient, path)

    @mock.patch('aodncore.pipeline.storage.sftp_makedirs')
    def test_sftp_mkdir_p_error_unknownerror(self, mock_sftp_makedirs):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        mock_sftp_makedirs.side_effect = EnvironmentError()
        with self.assertRaises(EnvironmentError):
            sftp_mkdir_p(sftpclient, path)


class TestBaseStorageBroker(BaseTestCase):
    def test_delete_fail(self):
        collection = get_upload_collection(delete=True)
        broker = NullStorageBroker("/", fail=True)
        with self.assertRaises(FileDeleteFailedError):
            broker.delete(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')

        self.assertFalse(collection[0].is_stored)

    def test_delete_success(self):
        collection = get_upload_collection(delete=True)
        broker = NullStorageBroker("/")
        broker.delete(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')
        self.assertTrue(collection[0].is_stored)

    def test_upload_fail(self):
        collection = get_upload_collection()
        broker = NullStorageBroker("/", fail=True)
        with self.assertRaises(FileUploadFailedError):
            broker.upload(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')

    def test_upload_success(self):
        collection = get_upload_collection()
        broker = NullStorageBroker("/")
        broker.upload(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')
        self.assertTrue(collection[0].is_stored)

    def test_set_is_overwrite_with_no_action_file(self):
        collection = get_upload_collection()

        temp_file = PipelineFile(self.temp_nc_file)
        self.assertIsNone(temp_file.dest_path)
        self.assertIs(temp_file.publish_type, PipelineFilePublishType.NO_ACTION)
        collection.add(temp_file)

        broker = NullStorageBroker("/")
        try:
            broker.set_is_overwrite(collection)
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))


class TestLocalFileStorageBroker(BaseTestCase):
    @mock.patch('aodncore.pipeline.storage.mkdir_p')
    @mock.patch('aodncore.pipeline.storage.safe_copy_file')
    def test_upload(self, mock_safe_copy_file, mock_mkdir_p):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        file_storage_broker = LocalFileStorageBroker('/tmp/probably/doesnt/exist/upload', None, self.test_logger)
        file_storage_broker.upload(collection)

        netcdf_dest_path = os.path.join(file_storage_broker.prefix, netcdf_file.dest_path)
        netcdf_dest_dir = os.path.dirname(netcdf_dest_path)
        png_dest_path = os.path.join(file_storage_broker.prefix, png_file.dest_path)
        png_dest_dir = os.path.dirname(png_dest_path)
        ico_dest_path = os.path.join(file_storage_broker.prefix, ico_file.dest_path)
        ico_dest_dir = os.path.dirname(ico_dest_path)
        unknown_dest_path = os.path.join(file_storage_broker.prefix, unknown_file.dest_path)
        unknown_dest_dir = os.path.dirname(unknown_dest_path)

        self.assertEqual(mock_mkdir_p.call_count, 4)
        mock_mkdir_p.assert_any_call(netcdf_dest_dir)
        mock_mkdir_p.assert_any_call(png_dest_dir)
        mock_mkdir_p.assert_any_call(ico_dest_dir)
        mock_mkdir_p.assert_any_call(unknown_dest_dir)

        self.assertEqual(mock_safe_copy_file.call_count, 4)
        mock_safe_copy_file.assert_any_call(netcdf_file.src_path, netcdf_dest_path, overwrite=True)
        mock_safe_copy_file.assert_any_call(png_file.src_path, png_dest_path, overwrite=True)
        mock_safe_copy_file.assert_any_call(ico_file.src_path, ico_dest_path, overwrite=True)
        mock_safe_copy_file.assert_any_call(unknown_file.src_path, unknown_dest_path, overwrite=True)

        self.assertTrue(all(p.is_stored for p in collection))

    @mock.patch('aodncore.pipeline.storage.rm_f')
    def test_delete(self, mock_rm_f):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        file_storage_broker = LocalFileStorageBroker('/tmp/probably/doesnt/exist/upload', None, self.test_logger)
        file_storage_broker.delete(collection)

        netcdf_dest_path = os.path.join(file_storage_broker.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(file_storage_broker.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(file_storage_broker.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(file_storage_broker.prefix, unknown_file.dest_path)

        self.assertEqual(mock_rm_f.call_count, 4)
        mock_rm_f.assert_any_call(netcdf_dest_path)
        mock_rm_f.assert_any_call(png_dest_path)
        mock_rm_f.assert_any_call(ico_dest_path)
        mock_rm_f.assert_any_call(unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))


class TestS3StorageBroker(BaseTestCase):
    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_invalid_bucket(self, mock_boto3):
        collection = get_upload_collection()

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        dummy_error = ClientError({'Error': {'Code': 'ServiceUnavailable'}}, 'ListObjects')
        s3_storage_broker.s3_client.head_bucket.side_effect = dummy_error

        with self.assertRaises(InvalidStoreUrlError):
            # mock out sleep to avoid long and unnecessary waiting during tests
            with mock.patch('aodncore.util.external.retry.api.time.sleep', new=lambda x: None):
                s3_storage_broker.upload(collection)

        self.assertEqual(s3_storage_broker.s3_client.head_bucket.call_count, s3_storage_broker.retry_kwargs['tries'])

        s3_storage_broker.s3_client.head_bucket.assert_called_with(Bucket=dummy_bucket)

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_set_is_overwrite_not_present_s3(self, mock_boto3):
        collection = get_upload_collection()
        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)
        s3_storage_broker.set_is_overwrite(collection)
        self.assertEqual(s3_storage_broker.s3_client.list_objects_v2.call_count, 4)
        self.assertFalse(any(f.is_overwrite for f in collection))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_set_is_overwrite_present_s3(self, mock_boto3):
        collection = get_upload_collection()
        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)
        dest_path = os.path.join('subdirectory', 'targetfile.nc')
        abs_path = os.path.join(dummy_prefix, dest_path)
        s3_storage_broker.s3_client.list_objects_v2.return_value = {'Contents': [{'Key': abs_path}]}
        s3_storage_broker.set_is_overwrite(collection)
        self.assertEqual(s3_storage_broker.s3_client.list_objects_v2.call_count, 4)
        self.assertTrue(all(f.is_overwrite for f in collection.filter_by_attribute_value('dest_path', dest_path)))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_set_is_overwrite_prefix_present_s3(self, mock_boto3):
        collection = get_upload_collection()
        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)
        abs_path_prefix = os.path.join(dummy_prefix, 'subdirectory')
        dest_path = os.path.join('subdirectory', 'targetfile.nc')
        abs_path = os.path.join(dummy_prefix, dest_path)
        s3_storage_broker.s3_client.list_objects_v2.return_value = {'Prefix': abs_path_prefix,
                                                                    'Contents': [{'Key': abs_path}]}
        s3_storage_broker.set_is_overwrite(collection)
        self.assertEqual(s3_storage_broker.s3_client.list_objects_v2.call_count, 4)
        self.assertTrue(all(f.is_overwrite for f in collection.filter_by_attribute_value('dest_path', dest_path)))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_upload(self, mock_boto3):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.storage.open', mock.mock_open(read_data='')) as m:
            s3_storage_broker.upload(collection)
        self.assertEqual(m.call_count, 4)
        m.assert_any_call(netcdf_file.src_path, 'rb')
        m.assert_any_call(png_file.src_path, 'rb')
        m.assert_any_call(ico_file.src_path, 'rb')
        m.assert_any_call(unknown_file.src_path, 'rb')

        netcdf_dest_path = os.path.join(dummy_prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(dummy_prefix, png_file.dest_path)
        ico_dest_path = os.path.join(dummy_prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(dummy_prefix, unknown_file.dest_path)

        s3_storage_broker.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)

        self.assertEqual(s3_storage_broker.s3_client.upload_fileobj.call_count, 4)

        s3_storage_broker.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=netcdf_dest_path,
                                                                   ExtraArgs={
                                                                       'ContentType': 'application/octet-stream'})

        s3_storage_broker.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=png_dest_path,
                                                                   ExtraArgs={'ContentType': 'image/png'})

        s3_storage_broker.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=ico_dest_path,
                                                                   ExtraArgs={
                                                                       'ContentType': 'image/vnd.microsoft.icon'})

        s3_storage_broker.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=unknown_dest_path,
                                                                   ExtraArgs={
                                                                       'ContentType': 'application/octet-stream'})

        self.assertTrue(all(p.is_stored for p in collection))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_delete(self, mock_boto3):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.storage.open', mock.mock_open(read_data='')) as m:
            s3_storage_broker.delete(collection)
        m.assert_not_called()

        netcdf_dest_path = os.path.join(s3_storage_broker.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(s3_storage_broker.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(s3_storage_broker.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(s3_storage_broker.prefix, unknown_file.dest_path)

        s3_storage_broker.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)

        self.assertEqual(s3_storage_broker.s3_client.delete_object.call_count, 4)
        s3_storage_broker.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=netcdf_dest_path)
        s3_storage_broker.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=png_dest_path)
        s3_storage_broker.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=ico_dest_path)
        s3_storage_broker.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))


# noinspection PyUnusedLocal
class TestSftpStorageBroker(BaseTestCase):
    @mock.patch('aodncore.pipeline.storage.SSHClient')
    @mock.patch('aodncore.pipeline.storage.AutoAddPolicy')
    def test_init(self, mock_autoaddpolicy, mock_sshclient):
        sftp_storage_broker = SftpStorageBroker('', '', None, self.test_logger)

        mock_sshclient.assert_called_once_with()
        sftp_storage_broker._sshclient.set_missing_host_key_policy.assert_called_once_with(mock_autoaddpolicy())

    @mock.patch('aodncore.pipeline.storage.SSHClient')
    @mock.patch('aodncore.pipeline.storage.AutoAddPolicy')
    def test_upload(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_storage_broker = SftpStorageBroker(dummy_server, dummy_prefix, None, self.test_logger)

        with mock.patch('aodncore.pipeline.storage.open', mock.mock_open(read_data='')) as m:
            sftp_storage_broker.upload(collection)

        sftp_storage_broker._sshclient.connect.assert_called_once_with(sftp_storage_broker.server)

        netcdf_dest_path = os.path.join(sftp_storage_broker.prefix, netcdf_file.dest_path)
        netcdf_dest_dir = os.path.dirname(netcdf_dest_path)
        png_dest_path = os.path.join(sftp_storage_broker.prefix, png_file.dest_path)
        png_dest_dir = os.path.dirname(png_dest_path)
        ico_dest_path = os.path.join(sftp_storage_broker.prefix, ico_file.dest_path)
        ico_dest_dir = os.path.dirname(ico_dest_path)
        unknown_dest_path = os.path.join(sftp_storage_broker.prefix, unknown_file.dest_path)
        unknown_dest_dir = os.path.dirname(unknown_dest_path)

        self.assertEqual(sftp_storage_broker.sftp_client.mkdir.call_count, 4)
        sftp_storage_broker.sftp_client.mkdir.assert_any_call(netcdf_dest_dir, 0o755)
        sftp_storage_broker.sftp_client.mkdir.assert_any_call(png_dest_dir, 0o755)
        sftp_storage_broker.sftp_client.mkdir.assert_any_call(ico_dest_dir, 0o755)
        sftp_storage_broker.sftp_client.mkdir.assert_any_call(unknown_dest_dir, 0o755)

        self.assertEqual(sftp_storage_broker.sftp_client.putfo.call_count, 4)
        sftp_storage_broker.sftp_client.putfo.assert_any_call(m(), netcdf_dest_path, confirm=True)
        sftp_storage_broker.sftp_client.putfo.assert_any_call(m(), png_dest_path, confirm=True)
        sftp_storage_broker.sftp_client.putfo.assert_any_call(m(), ico_dest_path, confirm=True)
        sftp_storage_broker.sftp_client.putfo.assert_any_call(m(), unknown_dest_path, confirm=True)

        self.assertTrue(all(p.is_stored for p in collection))

    @mock.patch('aodncore.pipeline.storage.SSHClient')
    @mock.patch('aodncore.pipeline.storage.AutoAddPolicy')
    def test_delete(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_storage_broker = SftpStorageBroker(dummy_server, dummy_prefix, None, self.test_logger)
        sftp_storage_broker.delete(collection)

        sftp_storage_broker._sshclient.connect.assert_called_once_with(sftp_storage_broker.server)

        netcdf_dest_path = os.path.join(sftp_storage_broker.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(sftp_storage_broker.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(sftp_storage_broker.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(sftp_storage_broker.prefix, unknown_file.dest_path)

        self.assertEqual(sftp_storage_broker.sftp_client.remove.call_count, 4)
        sftp_storage_broker.sftp_client.remove.assert_any_call(netcdf_dest_path)
        sftp_storage_broker.sftp_client.remove.assert_any_call(png_dest_path)
        sftp_storage_broker.sftp_client.remove.assert_any_call(ico_dest_path)
        sftp_storage_broker.sftp_client.remove.assert_any_call(unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))
