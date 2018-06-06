import errno
import os
import tempfile
from uuid import uuid4

import datetime
from botocore.exceptions import ClientError
from dateutil.tz import tzutc

from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import InvalidStoreUrlError, StorageBrokerError
from aodncore.pipeline.storage import (get_storage_broker, sftp_path_exists, sftp_makedirs, sftp_mkdir_p,
                                       LocalFileStorageBroker, S3StorageBroker, SftpStorageBroker)
from aodncore.testlib import BaseTestCase, NullStorageBroker, get_nonexistent_path, mock
from aodncore.util import TemporaryDirectory, format_exception
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
        file_url = 'file:///tmp/probably/doesnt/exist/upload'
        file_storage_broker = get_storage_broker(file_url)
        self.assertIsInstance(file_storage_broker, LocalFileStorageBroker)

        relative_file_url = 'file://tmp/probably/doesnt/exist/upload'
        with self.assertRaises(InvalidStoreUrlError):
            _ = get_storage_broker(relative_file_url)

        s3_url = "s3://{dummy_bucket}/{dummy_prefix}".format(dummy_bucket=str(uuid4()), dummy_prefix=str(uuid4()))
        s3_storage_broker = get_storage_broker(s3_url)
        self.assertIsInstance(s3_storage_broker, S3StorageBroker)

        sftp_url = "sftp://{dummy_host}/{dummy_path}".format(dummy_host=str(uuid4()), dummy_path=str(uuid4()))
        sftp_storage_broker = get_storage_broker(sftp_url)
        self.assertIsInstance(sftp_storage_broker, SftpStorageBroker)

        with self.assertRaises(InvalidStoreUrlError):
            _ = get_storage_broker('invalid_url')

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
        with self.assertRaises(StorageBrokerError):
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
        with self.assertRaises(StorageBrokerError):
            broker.upload(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')

    def test_upload_success(self):
        collection = get_upload_collection()
        broker = NullStorageBroker("/")
        broker.upload(pipeline_files=collection, is_stored_attr='is_stored', dest_path_attr='dest_path')
        self.assertTrue(collection[0].is_stored)

    def test_query_fail(self):
        broker = NullStorageBroker("/", fail=True)
        with self.assertRaises(StorageBrokerError):
            broker.query('')

    def test_set_is_overwrite_with_unset_file(self):
        collection = get_upload_collection()

        temp_file = PipelineFile(self.temp_nc_file)
        self.assertIsNone(temp_file.dest_path)
        self.assertIs(temp_file.publish_type, PipelineFilePublishType.UNSET)
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

        file_storage_broker = LocalFileStorageBroker('/tmp/probably/doesnt/exist/upload')
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

        file_storage_broker = LocalFileStorageBroker('/tmp/probably/doesnt/exist/upload')
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

    def test_directory_query(self):
        with TemporaryDirectory() as d:
            subdir = os.path.join(d, 'subdir')
            os.mkdir(subdir)
            _, temp_file1 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)
            _, temp_file2 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)
            _, temp_file3 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)

            file_storage_broker = LocalFileStorageBroker(d)
            result = file_storage_broker.query('subdir/')

        self.assertItemsEqual(result.keys(), [temp_file1, temp_file2, temp_file3])
        self.assertTrue(all(isinstance(v['last_modified'], datetime.datetime) for k, v in result.items()))
        self.assertTrue(all(isinstance(v['size'], int) for k, v in result.items()))

    def test_prefix_query(self):
        with TemporaryDirectory() as d:
            subdir = os.path.join(d, 'subdir')
            os.mkdir(subdir)
            _, temp_file1 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)
            _, temp_file2 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)
            _, temp_file3 = tempfile.mkstemp(suffix='.txt', prefix='asdfgh', dir=subdir)
            _, temp_file4 = tempfile.mkstemp(suffix='.txt', prefix='asdfgh', dir=subdir)

            file_storage_broker = LocalFileStorageBroker(d)
            result = file_storage_broker.query('subdir/qwerty')

        self.assertItemsEqual(result.keys(), [temp_file1, temp_file2])
        self.assertTrue(all(isinstance(v['last_modified'], datetime.datetime) for k, v in result.items()))
        self.assertTrue(all(isinstance(v['size'], int) for k, v in result.items()))

    def test_query_empty(self):
        with TemporaryDirectory() as d:
            subdir = os.path.join(d, 'subdir')
            os.mkdir(subdir)
            _, temp_file3 = tempfile.mkstemp(suffix='.txt', prefix='asdfgh', dir=subdir)

            file_storage_broker = LocalFileStorageBroker(d)

            try:
                result = file_storage_broker.query('subdir/qwerty')
            except Exception as e:
                raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        self.assertDictEqual(result, {})

    @mock.patch('aodncore.pipeline.storage.os.stat')
    def test_query_error(self, mock_stat):
        dummy_error = IOError()
        mock_stat.side_effect = dummy_error

        with TemporaryDirectory() as d:
            subdir = os.path.join(d, 'subdir')
            os.mkdir(subdir)
            _, temp_file1 = tempfile.mkstemp(suffix='.txt', prefix='qwerty', dir=subdir)

            file_storage_broker = LocalFileStorageBroker(d)
            with self.assertRaises(StorageBrokerError):
                _ = file_storage_broker.query('subdir/qwerty')


class TestS3StorageBroker(BaseTestCase):
    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_invalid_bucket(self, mock_boto3):
        collection = get_upload_collection()

        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)

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
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)
        s3_storage_broker.set_is_overwrite(collection)
        self.assertEqual(s3_storage_broker.s3_client.list_objects_v2.call_count, 4)
        self.assertFalse(any(f.is_overwrite for f in collection))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_set_is_overwrite_present_s3(self, mock_boto3):
        collection = get_upload_collection()
        dummy_bucket = str(uuid4())
        dummy_prefix = str(uuid4())
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)
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
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)
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
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)

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
        s3_storage_broker = S3StorageBroker(dummy_bucket, dummy_prefix)

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

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_directory_query(self, mock_boto3):
        mock_boto3.client().list_objects_v2.return_value = {'Contents': [
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/DSTO_MD_CEPSTUV_20140213T050333Z_SL085_FV01_timeseries_END-20140312T003551Z.nc',
             u'Size': 39203028},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213.kml',
             u'Size': 48877},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_CNDC.jpg',
             u'Size': 104238},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_PSAL.jpg',
             u'Size': 115044},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_TEMP.jpg',
             u'Size': 106141}]}

        s3_storage_broker = S3StorageBroker('imos-data', '')
        result = s3_storage_broker.query('Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/')

        self.assertItemsEqual(result.keys(),
                              [l['Key'] for l in mock_boto3.client().list_objects_v2.return_value['Contents']])
        self.assertTrue(all(isinstance(v['last_modified'], datetime.datetime) for k, v in result.items()))
        self.assertTrue(all(isinstance(v['size'], int) for k, v in result.items()))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_prefix_query(self, mock_boto3):
        mock_boto3.client().list_objects_v2.return_value = {'Contents': [
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/DSTO_MD_CEPSTUV_20140213T050333Z_SL085_FV01_timeseries_END-20140312T003551Z.nc',
             u'Size': 39203028},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213.kml',
             u'Size': 48877},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_CNDC.jpg',
             u'Size': 104238},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_PSAL.jpg',
             u'Size': 115044},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonA20140213/PerthCanyonA20140213_TEMP.jpg',
             u'Size': 106141},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/DSTO_MD_CEPSTUV_20140213T050730Z_SL090_FV01_timeseries_END-20140221T102451Z.nc',
             u'Size': 11622292},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213.kml',
             u'Size': 21574},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 9, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_CNDC.jpg',
             u'Size': 131749},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 10, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_PSAL.jpg',
             u'Size': 139704},
            {u'LastModified': datetime.datetime(2016, 4, 27, 2, 30, 8, tzinfo=tzutc()),
             u'Key': 'Department_of_Defence/DSTG/slocum_glider/PerthCanyonB20140213/PerthCanyonB20140213_TEMP.jpg',
             u'Size': 132122}]}

        s3_storage_broker = S3StorageBroker('imos-data', '')
        result = s3_storage_broker.query('Department_of_Defence/DSTG/slocum_glider/Perth')

        self.assertItemsEqual(result.keys(),
                              [l['Key'] for l in mock_boto3.client().list_objects_v2.return_value['Contents']])
        self.assertTrue(all(isinstance(v['last_modified'], datetime.datetime) for k, v in result.items()))
        self.assertTrue(all(isinstance(v['size'], int) for k, v in result.items()))

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_query_empty(self, mock_boto3):
        mock_boto3.client().list_objects_v2.return_value = {}

        s3_storage_broker = S3StorageBroker('imos-data', '')
        try:
            result = s3_storage_broker.query('Department_of_Defence/DSTG/slocum_glider/Perth')
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        self.assertDictEqual(result, {})

    @mock.patch('aodncore.pipeline.storage.boto3')
    def test_query_error(self, mock_boto3):
        dummy_error = ClientError({'Error': {'Code': 'ServiceUnavailable'}}, 'ListObjects')
        mock_boto3.client().list_objects_v2.side_effect = dummy_error

        s3_storage_broker = S3StorageBroker('imos-data', '')
        with self.assertRaises(StorageBrokerError):
            _ = s3_storage_broker.query('Department_of_Defence/DSTG/slocum_glider/Perth')


# noinspection PyUnusedLocal
class TestSftpStorageBroker(BaseTestCase):
    @mock.patch('aodncore.pipeline.storage.SSHClient')
    @mock.patch('aodncore.pipeline.storage.AutoAddPolicy')
    def test_init(self, mock_autoaddpolicy, mock_sshclient):
        sftp_storage_broker = SftpStorageBroker('', '')

        mock_sshclient.assert_called_once_with()
        sftp_storage_broker._sshclient.set_missing_host_key_policy.assert_called_once_with(mock_autoaddpolicy())

    @mock.patch('aodncore.pipeline.storage.SSHClient')
    @mock.patch('aodncore.pipeline.storage.AutoAddPolicy')
    def test_upload(self, mock_autoaddpolicy, mock_sshclient):
        collection = get_upload_collection()
        netcdf_file, png_file, ico_file, unknown_file = collection

        dummy_server = str(uuid4())
        dummy_prefix = "/tmp/{uuid}".format(uuid=str(uuid4()))

        sftp_storage_broker = SftpStorageBroker(dummy_server, dummy_prefix)

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

        sftp_storage_broker = SftpStorageBroker(dummy_server, dummy_prefix)
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
