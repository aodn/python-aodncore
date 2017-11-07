import errno
import os
from uuid import uuid4

from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import FileDeleteFailedError, FileUploadFailedError, InvalidUploadUrlError
from aodncore.pipeline.steps.upload import (get_upload_runner, sftp_path_exists, sftp_makedirs, sftp_mkdir_p,
                                            BaseUploadRunner, FileUploadRunner, S3UploadRunner, SftpUploadRunner)
from test_aodncore.testlib import BaseTestCase, get_nonexistent_path, mock, MOCK_LOGGER

TESTDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testdata')
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


def get_upload_collection(delete=False):
    pipeline_file = PipelineFile(GOOD_NC, is_deletion=delete)
    pipeline_file.publish_type = PipelineFilePublishType.DELETE_ONLY if delete else PipelineFilePublishType.UPLOAD_ONLY
    pipeline_file.dest_path = 'subdirectory/targetfile.nc'
    collection = PipelineFileCollection([pipeline_file])
    return collection


# noinspection PyUnusedLocal
class TestPipelineStepsUpload(BaseTestCase):
    def test_get_upload_runner(self):
        file_uri = 'file:///tmp/probably/doesnt/exist/upload'
        file_upload_runner = get_upload_runner(file_uri, None, MOCK_LOGGER)
        self.assertIsInstance(file_upload_runner, FileUploadRunner)

        s3_uri = "s3://{dummy_bucket}/{dummy_prefix}".format(dummy_bucket=str(uuid4()), dummy_prefix=str(uuid4()))
        s3_upload_runner = get_upload_runner(s3_uri, None, MOCK_LOGGER)
        self.assertIsInstance(s3_upload_runner, S3UploadRunner)

        sftp_uri = "sftp://{dummy_host}/{dummy_path}".format(dummy_host=str(uuid4()), dummy_path=str(uuid4()))
        sftp_upload_runner = get_upload_runner(sftp_uri, None, MOCK_LOGGER)
        self.assertIsInstance(sftp_upload_runner, SftpUploadRunner)

        with self.assertRaises(InvalidUploadUrlError):
            _ = get_upload_runner('invalid_uri', None, MOCK_LOGGER)

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


class NullUploadRunner(BaseUploadRunner):
    def __init__(self, prefix, fail):
        super(NullUploadRunner, self).__init__(None, MOCK_LOGGER)
        self.prefix = prefix
        self.fail = fail

    def _delete_file(self, pipeline_file):
        if self.fail:
            raise NotImplementedError

    def _post_run_hook(self):
        pass

    def _pre_run_hook(self):
        pass

    def _upload_file(self, pipeline_file):
        if self.fail:
            raise NotImplementedError

    def _get_absolute_dest_uri(self, pipeline_file):
        return "null://{dest_path}".format(dest_path=pipeline_file.dest_path)


class TestBaseUploadRunner(BaseTestCase):
    def test_delete_fail(self):
        collection = get_upload_collection(delete=True)
        runner = NullUploadRunner("/", fail=True)
        with self.assertRaises(FileDeleteFailedError):
            runner.run(collection)

        self.assertFalse(collection[0].is_stored)

    def test_delete_success(self):
        collection = get_upload_collection(delete=True)
        runner = NullUploadRunner("/", fail=False)
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)

    def test_upload_fail(self):
        collection = get_upload_collection()
        runner = NullUploadRunner("/", fail=True)
        with self.assertRaises(FileUploadFailedError):
            runner.run(collection)

    def test_upload_success(self):
        collection = get_upload_collection()
        runner = NullUploadRunner("/", fail=False)
        runner.run(collection)
        self.assertTrue(collection[0].is_stored)


class TestFileUploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.mkdir_p')
    @mock.patch('aodncore.pipeline.steps.upload.safe_copy_file')
    def test_upload(self, mock_safe_copy_file, mock_mkdir_p):
        collection = get_upload_collection()
        pipeline_file = collection[0]

        file_upload_runner = FileUploadRunner('/tmp/probably/doesnt/exist/upload', None, MOCK_LOGGER)
        file_upload_runner.run(collection)

        dest_path = os.path.join(file_upload_runner.prefix, pipeline_file.dest_path)
        dest_dir = os.path.dirname(dest_path)

        mock_mkdir_p.assert_called_once_with(dest_dir)
        mock_safe_copy_file.assert_called_once_with(pipeline_file.src_path, dest_path, overwrite=True)
        self.assertTrue(pipeline_file.is_stored)

    @mock.patch('aodncore.pipeline.steps.upload.rm_f')
    def test_delete(self, mock_rm_f):
        collection = get_upload_collection(delete=True)
        pipeline_file = collection[0]

        file_upload_runner = FileUploadRunner('/tmp/probably/doesnt/exist/upload', None, MOCK_LOGGER)
        file_upload_runner.run(collection)

        dest_path = os.path.join(file_upload_runner.prefix, pipeline_file.dest_path)

        mock_rm_f.assert_called_once_with(dest_path)
        self.assertTrue(pipeline_file.is_stored)


class TestS3UploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_invalid_bucket(self, mock_boto3):
        collection = get_upload_collection()

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, MOCK_LOGGER)

        mock_boto3.client.assert_called_once_with('s3')

        s3_upload_runner.s3_client.head_bucket.side_effect = InvalidUploadUrlError()
        with self.assertRaises(InvalidUploadUrlError):
            s3_upload_runner.run(collection)

        s3_upload_runner.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)

    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_upload(self, mock_boto3):
        collection = get_upload_collection()
        pipeline_file = collection[0]

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, MOCK_LOGGER)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            s3_upload_runner.run(collection)
        m.assert_called_once_with(pipeline_file.src_path, 'rb')

        dest_path = os.path.join(s3_upload_runner.prefix, pipeline_file.dest_path)

        s3_upload_runner.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)
        s3_upload_runner.s3_client.upload_fileobj.assert_called_once_with(m(), Bucket=dummy_bucket, Key=dest_path)

        self.assertTrue(pipeline_file.is_stored)

    @mock.patch('aodncore.pipeline.steps.upload.boto3')
    def test_delete(self, mock_boto3):
        collection = get_upload_collection(delete=True)
        pipeline_file = collection[0]

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_upload_runner = S3UploadRunner(dummy_bucket, dummy_prefix, None, MOCK_LOGGER)

        mock_boto3.client.assert_called_once_with('s3')

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            s3_upload_runner.run(collection)
        m.assert_not_called()

        dest_path = os.path.join(s3_upload_runner.prefix, pipeline_file.dest_path)

        s3_upload_runner.s3_client.head_bucket.assert_called_once_with(Bucket=dummy_bucket)
        s3_upload_runner.s3_client.delete_object.assert_called_once_with(Bucket=dummy_bucket, Key=dest_path)

        self.assertTrue(pipeline_file.is_stored)


# noinspection PyUnusedLocal
class TestSftpUploadRunner(BaseTestCase):
    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_init(self, mock_autoaddpolicy, mock_sshclient):
        sftp_upload_runner = SftpUploadRunner('', '', None, MOCK_LOGGER)

        mock_sshclient.assert_called_once_with()
        sftp_upload_runner._sshclient.set_missing_host_key_policy.assert_called_once_with(mock_autoaddpolicy())

    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_upload(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection()
        pipeline_file = collection[0]

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_upload_runner = SftpUploadRunner(dummy_server, dummy_prefix, None, MOCK_LOGGER)

        with mock.patch('aodncore.pipeline.steps.upload.open', mock.mock_open(read_data='')) as m:
            sftp_upload_runner.run(collection)

        sftp_upload_runner._sshclient.connect.assert_called_once_with(sftp_upload_runner.server)

        dest_path = os.path.join(sftp_upload_runner.prefix, pipeline_file.dest_path)
        parent_dir = os.path.dirname(dest_path)
        grandparent_dir = os.path.dirname(parent_dir)

        sftp_upload_runner.sftp_client.stat.assert_called_once_with(grandparent_dir)
        sftp_upload_runner.sftp_client.mkdir.assert_called_once_with(parent_dir, 0o755)

        sftp_upload_runner.sftp_client.putfo.assert_called_once_with(m(), dest_path, confirm=True)

        self.assertTrue(pipeline_file.is_stored)

    @mock.patch('aodncore.pipeline.steps.upload.SSHClient')
    @mock.patch('aodncore.pipeline.steps.upload.AutoAddPolicy')
    def test_delete(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection(delete=True)
        pipeline_file = collection[0]

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_upload_runner = SftpUploadRunner(dummy_server, dummy_prefix, None, MOCK_LOGGER)
        sftp_upload_runner.run(collection)

        sftp_upload_runner._sshclient.connect.assert_called_once_with(sftp_upload_runner.server)

        dest_path = os.path.join(sftp_upload_runner.prefix, pipeline_file.dest_path)

        sftp_upload_runner.sftp_client.remove.assert_called_once_with(dest_path)

        self.assertTrue(pipeline_file.is_stored)
