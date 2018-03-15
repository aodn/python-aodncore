import errno
import os
from uuid import uuid4

from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import FileDeleteFailedError, FileUploadFailedError, InvalidUploadUrlError
from aodncore.pipeline.steps.upload import (get_upload_runner, sftp_path_exists, sftp_makedirs, sftp_mkdir_p,
                                            FileUploadRunner, S3UploadRunner, SftpUploadRunner)
from aodncore.testlib import BaseTestCase, NullUploadRunner, get_nonexistent_path, mock
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
class TestPipelineStepsUpload(BaseTestCase):
    def test_get_upload_runner(self):
        file_uri = 'file:///tmp/probably/doesnt/exist/upload'
        file_upload_runner = get_upload_runner(file_uri, None, self.test_logger)
        self.assertIsInstance(file_upload_runner, FileUploadRunner)

        s3_uri = "s3://{dummy_bucket}/{dummy_prefix}".format(dummy_bucket=str(uuid4()), dummy_prefix=str(uuid4()))
        s3_upload_runner = get_upload_runner(s3_uri, None, self.test_logger)
        self.assertIsInstance(s3_upload_runner, S3UploadRunner)

        sftp_uri = "sftp://{dummy_host}/{dummy_path}".format(dummy_host=str(uuid4()), dummy_path=str(uuid4()))
        sftp_upload_runner = get_upload_runner(sftp_uri, None, self.test_logger)
        self.assertIsInstance(sftp_upload_runner, SftpUploadRunner)

        with self.assertRaises(InvalidUploadUrlError):
            _ = get_upload_runner('invalid_uri', None, self.test_logger)

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

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    def test_sftp_makedirs_parent_dotsegment(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        path_with_dot = os.path.join(path, '.')
        mode = 0o755

        mock_sftp_path_exists.return_value = False

        sftp_makedirs(sftpclient, path_with_dot)

        sftpclient.mkdir.assert_called_with(path, mode)
        self.assertEqual(sftpclient.mkdir.call_count, 9)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    def test_sftp_makedirs_parent_exists(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_path_exists.return_value = True

        sftp_makedirs(sftpclient, path)

        sftpclient.mkdir.assert_called_once_with(path, mode)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    def test_sftp_makedirs_parent_notexists(self, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_path_exists.return_value = False

        sftp_makedirs(sftpclient, path)

        sftpclient.mkdir.assert_called_with(path, mode)
        self.assertEqual(sftpclient.mkdir.call_count, 9)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    @mock.patch('aodncore.pipeline.steps.upload.sftp_makedirs')
    def test_sftp_mkdir_p_newdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        sftp_mkdir_p(sftpclient, path)

        mock_sftp_makedirs.assert_called_once_with(sftpclient, path, mode)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    @mock.patch('aodncore.pipeline.steps.upload.sftp_makedirs')
    def test_sftp_mkdir_p_ioerror_existingdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()
        mode = 0o755

        mock_sftp_makedirs.side_effect = IOError()
        mock_sftp_path_exists.return_value = True
        sftp_mkdir_p(sftpclient, path)

        mock_sftp_makedirs.assert_called_once_with(sftpclient, path, mode)
        mock_sftp_path_exists.assert_called_once_with(sftpclient, path)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_path_exists')
    @mock.patch('aodncore.pipeline.steps.upload.sftp_makedirs')
    def test_sftp_mkdir_p_error_ioerror_nonexistingdir(self, mock_sftp_makedirs, mock_sftp_path_exists):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        mock_sftp_path_exists.return_value = False
        mock_sftp_makedirs.side_effect = IOError()
        with self.assertRaises(IOError):
            sftp_mkdir_p(sftpclient, path)

    @mock.patch('aodncore.pipeline.steps.upload.sftp_makedirs')
    def test_sftp_mkdir_p_error_unknownerror(self, mock_sftp_makedirs):
        sftpclient = mock.MagicMock()
        path = get_nonexistent_path()

        mock_sftp_makedirs.side_effect = EnvironmentError()
        with self.assertRaises(EnvironmentError):
            sftp_mkdir_p(sftpclient, path)


class TestBaseUploadRunner(BaseTestCase):
    def test_delete_fail(self):
        collection = get_upload_collection(delete=True)
        runner = NullUploadRunner("/", fail=True)
        with self.assertRaises(FileDeleteFailedError):
            runner.run(collection)

        self.assertFalse(collection[0].is_stored)

    def test_delete_success(self):
        collection = get_upload_collection(delete=True)
        runner = NullUploadRunner("/")
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)

    def test_upload_fail(self):
        collection = get_upload_collection()
        runner = NullUploadRunner("/", fail=True)
        with self.assertRaises(FileUploadFailedError):
            runner.run(collection)

    def test_upload_success(self):
        collection = get_upload_collection()
        runner = NullUploadRunner("/")
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)

    def test_set_is_overwrite_with_no_action_file(self):
        collection = get_upload_collection()

        temp_file = PipelineFile(self.temp_nc_file)
        self.assertIsNone(temp_file.dest_path)
        self.assertIs(temp_file.publish_type, PipelineFilePublishType.NO_ACTION)
        collection.add(temp_file)

        runner = NullUploadRunner("/")
        try:
            runner.set_is_overwrite(collection)
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))


class TestFileUploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.mkdir_p')
    @mock.patch('aodncore.pipeline.steps.upload.safe_copy_file')
    def test_upload(self, mock_safe_copy_file, mock_mkdir_p):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        file_upload_runner = FileUploadRunner('/tmp/probably/doesnt/exist/upload', None, self.test_logger)
        file_upload_runner.run(collection)

        netcdf_dest_path = os.path.join(file_upload_runner.prefix, netcdf_file.dest_path)
        netcdf_dest_dir = os.path.dirname(netcdf_dest_path)
        png_dest_path = os.path.join(file_upload_runner.prefix, png_file.dest_path)
        png_dest_dir = os.path.dirname(png_dest_path)
        ico_dest_path = os.path.join(file_upload_runner.prefix, ico_file.dest_path)
        ico_dest_dir = os.path.dirname(ico_dest_path)
        unknown_dest_path = os.path.join(file_upload_runner.prefix, unknown_file.dest_path)
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

    @mock.patch('aodncore.pipeline.steps.upload.rm_f')
    def test_delete(self, mock_rm_f):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        file_upload_runner = FileUploadRunner('/tmp/probably/doesnt/exist/upload', None, self.test_logger)
        file_upload_runner.run(collection)

        netcdf_dest_path = os.path.join(file_upload_runner.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(file_upload_runner.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(file_upload_runner.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(file_upload_runner.prefix, unknown_file.dest_path)

        self.assertEqual(mock_rm_f.call_count, 4)
        mock_rm_f.assert_any_call(netcdf_dest_path)
        mock_rm_f.assert_any_call(png_dest_path)
        mock_rm_f.assert_any_call(ico_dest_path)
        mock_rm_f.assert_any_call(unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))


class TestS3UploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_invalid_bucket(self, mock_boto3):
        collection = get_upload_collection()

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        s3_upload_runner.s3_client.head_bucket.side_effect = InvalidUploadUrlError()
        with self.assertRaises(InvalidUploadUrlError):
            s3_upload_runner.run(collection)

        assert(s3_upload_runner.s3_client.head_bucket.call_count == 4)

        s3_upload_runner.s3_client.head_bucket.assert_called_with(Bucket=dummy_bucket)

    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_upload(self, mock_boto3):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            s3_upload_runner.run(collection)
        self.assertEqual(m.call_count, 4)
        m.assert_any_call(netcdf_file.src_path, 'rb')
        m.assert_any_call(png_file.src_path, 'rb')
        m.assert_any_call(ico_file.src_path, 'rb')
        m.assert_any_call(unknown_file.src_path, 'rb')

        netcdf_dest_path = os.path.join(dummy_prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(dummy_prefix, png_file.dest_path)
        ico_dest_path = os.path.join(dummy_prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(dummy_prefix, unknown_file.dest_path)

        s3_upload_runner.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)

        self.assertEqual(s3_upload_runner.s3_client.upload_fileobj.call_count, 4)

        s3_upload_runner.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=netcdf_dest_path,
                                                                  ExtraArgs={'ContentType': 'application/octet-stream'})

        s3_upload_runner.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=png_dest_path,
                                                                  ExtraArgs={'ContentType': 'image/png'})

        s3_upload_runner.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=ico_dest_path,
                                                                  ExtraArgs={'ContentType': 'image/vnd.microsoft.icon'})

        s3_upload_runner.s3_client.upload_fileobj.assert_any_call(m(), Bucket=dummy_bucket, Key=unknown_dest_path,
                                                                  ExtraArgs={'ContentType': 'application/octet-stream'})

        self.assertTrue(all(p.is_stored for p in collection))

    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_delete(self, mock_boto3):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, self.test_logger)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            s3_upload_runner.run(collection)
        m.assert_not_called()

        netcdf_dest_path = os.path.join(s3_upload_runner.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(s3_upload_runner.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(s3_upload_runner.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(s3_upload_runner.prefix, unknown_file.dest_path)

        s3_upload_runner.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)

        self.assertEqual(s3_upload_runner.s3_client.delete_object.call_count, 4)
        s3_upload_runner.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=netcdf_dest_path)
        s3_upload_runner.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=png_dest_path)
        s3_upload_runner.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=ico_dest_path)
        s3_upload_runner.s3_client.delete_object.assert_any_call(Bucket=dummy_bucket, Key=unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))


# noinspection PyUnusedLocal
class TestSftpUploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_init(self, mock_autoaddpolicy, mock_sshclient):
        sftp_upload_runner = SftpUploadRunner('', '', None, self.test_logger)

        mock_sshclient.assert_called_once_with()
        sftp_upload_runner._sshclient.set_missing_host_key_policy.assert_called_once_with(mock_autoaddpolicy())

    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_upload(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_upload_runner = SftpUploadRunner(dummy_server, dummy_prefix, None, self.test_logger)

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            sftp_upload_runner.run(collection)

        sftp_upload_runner._sshclient.connect.assert_called_once_with(sftp_upload_runner.server)

        netcdf_dest_path = os.path.join(sftp_upload_runner.prefix, netcdf_file.dest_path)
        netcdf_dest_dir = os.path.dirname(netcdf_dest_path)
        png_dest_path = os.path.join(sftp_upload_runner.prefix, png_file.dest_path)
        png_dest_dir = os.path.dirname(png_dest_path)
        ico_dest_path = os.path.join(sftp_upload_runner.prefix, ico_file.dest_path)
        ico_dest_dir = os.path.dirname(ico_dest_path)
        unknown_dest_path = os.path.join(sftp_upload_runner.prefix, unknown_file.dest_path)
        unknown_dest_dir = os.path.dirname(unknown_dest_path)

        self.assertEqual(sftp_upload_runner.sftp_client.mkdir.call_count, 4)
        sftp_upload_runner.sftp_client.mkdir.assert_any_call(netcdf_dest_dir, 0o755)
        sftp_upload_runner.sftp_client.mkdir.assert_any_call(png_dest_dir, 0o755)
        sftp_upload_runner.sftp_client.mkdir.assert_any_call(ico_dest_dir, 0o755)
        sftp_upload_runner.sftp_client.mkdir.assert_any_call(unknown_dest_dir, 0o755)

        self.assertEqual(sftp_upload_runner.sftp_client.putfo.call_count, 4)
        sftp_upload_runner.sftp_client.putfo.assert_any_call(m(), netcdf_dest_path, confirm=True)
        sftp_upload_runner.sftp_client.putfo.assert_any_call(m(), png_dest_path, confirm=True)
        sftp_upload_runner.sftp_client.putfo.assert_any_call(m(), ico_dest_path, confirm=True)
        sftp_upload_runner.sftp_client.putfo.assert_any_call(m(), unknown_dest_path, confirm=True)

        self.assertTrue(all(p.is_stored for p in collection))

    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_delete(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection(delete=True)
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_upload_runner = SftpUploadRunner(dummy_server, dummy_prefix, None, self.test_logger)
        sftp_upload_runner.run(collection)

        sftp_upload_runner._sshclient.connect.assert_called_once_with(sftp_upload_runner.server)

        netcdf_dest_path = os.path.join(sftp_upload_runner.prefix, netcdf_file.dest_path)
        png_dest_path = os.path.join(sftp_upload_runner.prefix, png_file.dest_path)
        ico_dest_path = os.path.join(sftp_upload_runner.prefix, ico_file.dest_path)
        unknown_dest_path = os.path.join(sftp_upload_runner.prefix, unknown_file.dest_path)

        self.assertEqual(sftp_upload_runner.sftp_client.remove.call_count, 4)
        sftp_upload_runner.sftp_client.remove.assert_any_call(netcdf_dest_path)
        sftp_upload_runner.sftp_client.remove.assert_any_call(png_dest_path)
        sftp_upload_runner.sftp_client.remove.assert_any_call(ico_dest_path)
        sftp_upload_runner.sftp_client.remove.assert_any_call(unknown_dest_path)

        self.assertTrue(all(p.is_stored for p in collection))
