import filecmp
import gzip
import os
import socket
import uuid
import zipfile
from io import open
from tempfile import mkdtemp, mkstemp

from aodncore.testlib import BaseTestCase, get_nonexistent_path
from aodncore.util import (extract_gzip, extract_zip, is_gzip_file, is_jpeg_file, is_netcdf_file, is_pdf_file,
                           is_png_file, is_tiff_file, is_zip_file, list_regular_files, find_file, mkdir_p, rm_f, rm_r,
                           rm_rf, safe_copy_file, safe_move_file, get_file_checksum, TemporaryDirectory)
from aodncore.util.misc import format_exception

from test_aodncore import TESTDATA_DIR

JPEG_FILE = os.path.join(TESTDATA_DIR, 'aodn.jpeg')
PDF_FILE = os.path.join(TESTDATA_DIR, 'aodn.pdf')
PNG_FILE = os.path.join(TESTDATA_DIR, 'invalid.png')
TIFF_FILE = os.path.join(TESTDATA_DIR, 'aodn.tiff')


class TestUtilFileOps(BaseTestCase):
    def test_extract_gzip(self):
        temp_file_content = str(uuid.uuid4()).encode('utf-8')

        temp_gz_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_gz_file = mkstemp(suffix='.gz', prefix=self.__class__.__name__, dir=self.temp_dir)

        with gzip.GzipFile(temp_gz_file, 'wb') as gz:
            gz.write(temp_file_content)

        extract_gzip(temp_gz_file, temp_gz_dir)
        expected_filename = os.path.basename(temp_gz_file).rstrip('.gz')

        self.assertIn(expected_filename, os.listdir(temp_gz_dir))
        with open(os.path.join(temp_gz_dir, expected_filename), 'rb') as f:
            temp_file_content2 = f.read()
        self.assertEqual(temp_file_content, temp_file_content2)

    def test_extract_zip(self):
        temp_file_name = str(uuid.uuid4())
        temp_file_content = str(uuid.uuid4())

        temp_zip_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_zip_file = mkstemp(suffix='.zip', prefix=self.__class__.__name__, dir=self.temp_dir)

        with zipfile.ZipFile(temp_zip_file, 'w', zipfile.ZIP_DEFLATED) as z:
            z.writestr(temp_file_name, temp_file_content)

        extract_zip(temp_zip_file, temp_zip_dir)

        self.assertIn(temp_file_name, os.listdir(temp_zip_dir))
        with open(os.path.join(temp_zip_dir, temp_file_name), 'r') as f:
            temp_file_content2 = f.readline()
        self.assertEqual(temp_file_content, temp_file_content2)

    def test_isjpegfile(self):
        self.assertTrue(is_jpeg_file(JPEG_FILE))
        self.assertFalse(is_jpeg_file(self.temp_nc_file))
        self.assertFalse(is_jpeg_file(PNG_FILE))

    def test_isnetcdffile(self):
        _, temp_other_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        with open(temp_other_file, 'w') as f:
            f.write(u'foobar')

        self.assertTrue(is_netcdf_file(self.temp_nc_file))
        self.assertFalse(is_netcdf_file(temp_other_file))

    def test_ispdffile(self):
        self.assertTrue(is_pdf_file(PDF_FILE))
        self.assertFalse(is_pdf_file(self.temp_nc_file))
        self.assertFalse(is_pdf_file(JPEG_FILE))

    def test_ispngfile(self):
        self.assertTrue(is_png_file(PNG_FILE))
        self.assertFalse(is_png_file(self.temp_nc_file))
        self.assertFalse(is_png_file(JPEG_FILE))

    def test_istifffile(self):
        self.assertTrue(is_tiff_file(TIFF_FILE))
        self.assertFalse(is_tiff_file(self.temp_nc_file))
        self.assertFalse(is_tiff_file(JPEG_FILE))

    def test_isgzipfile(self):
        temp_file_content = str(uuid.uuid4()).encode('utf-8')

        _, temp_gz_file = mkstemp(suffix='.zip', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_other_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        with gzip.open(temp_gz_file, 'w') as gz:
            gz.write(temp_file_content)

        with open(temp_other_file, 'w') as f:
            f.write(u'foobar')

        self.assertTrue(is_gzip_file(temp_gz_file))
        self.assertFalse(is_gzip_file(temp_other_file))

    def test_iszipfile(self):
        temp_file_name = str(uuid.uuid4())
        temp_file_content = str(uuid.uuid4())

        _, temp_zip_file = mkstemp(suffix='.zip', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_other_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        with zipfile.ZipFile(temp_zip_file, 'w', zipfile.ZIP_DEFLATED) as z:
            z.writestr(temp_file_name, temp_file_content)
        with open(temp_other_file, 'w') as f:
            f.write(u'foobar')

        self.assertTrue(is_zip_file(temp_zip_file))
        self.assertFalse(is_zip_file(temp_other_file))

    def test_list_regular_files(self):
        # regular file
        _, temp_regular_file1 = mkstemp(prefix='b' + self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file2 = mkstemp(prefix='a' + self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file3 = mkstemp(prefix='B' + self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file4 = mkstemp(prefix='A' + self.__class__.__name__, dir=self.temp_dir)

        # directory
        temp_subdirectory = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        # symlinks
        temp_file_symlink = "{source_file}.symlink".format(source_file=temp_regular_file1)
        temp_dir_symlink = "{source_dir}.symlink".format(source_dir=temp_subdirectory)
        os.symlink(temp_regular_file1, temp_file_symlink)
        os.symlink(temp_subdirectory, temp_dir_symlink)

        # unix socket
        _, temp_socket_file = mkstemp(suffix='.sock', prefix=self.__class__.__name__, dir=self.temp_dir)
        rm_f(temp_socket_file)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(temp_socket_file)

        reference_list = [temp_regular_file4, temp_regular_file3, temp_regular_file2, temp_regular_file1]
        dir_entries = list(list_regular_files(self.temp_dir))

        self.assertListEqual(dir_entries, reference_list)

        with self.assertRaises(ValueError):
            list_regular_files(self.temp_dir, sort_key='non_callable_key')

    def test_list_regular_files_recursive(self):
        temp_subdirectory_0_1 = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_subdirectory_0_2 = mkdtemp(prefix='B' + self.__class__.__name__, dir=self.temp_dir)
        temp_subdirectory_0_3 = mkdtemp(prefix='a' + self.__class__.__name__, dir=self.temp_dir)

        _, temp_regular_file_0_1 = mkstemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        temp_subdirectory_1_1 = mkdtemp(prefix='a' + self.__class__.__name__, dir=temp_subdirectory_0_1)

        _, temp_regular_file_1_1 = mkstemp(prefix='b' + self.__class__.__name__, dir=temp_subdirectory_0_1)
        _, temp_regular_file_1_2 = mkstemp(prefix='A' + self.__class__.__name__, dir=temp_subdirectory_0_1)
        _, temp_regular_file_1_3 = mkstemp(prefix='B' + self.__class__.__name__, dir=temp_subdirectory_0_2)
        _, temp_regular_file_1_4 = mkstemp(prefix='a' + self.__class__.__name__, dir=temp_subdirectory_0_2)
        _, temp_regular_file_1_5 = mkstemp(prefix='b' + self.__class__.__name__, dir=temp_subdirectory_0_3)
        _, temp_regular_file_1_6 = mkstemp(prefix='A' + self.__class__.__name__, dir=temp_subdirectory_0_3)

        _, temp_regular_file_2_1 = mkstemp(prefix='b' + self.__class__.__name__, dir=temp_subdirectory_1_1)
        _, temp_regular_file_2_2 = mkstemp(prefix='a' + self.__class__.__name__, dir=temp_subdirectory_1_1)
        _, temp_regular_file_2_3 = mkstemp(prefix='B' + self.__class__.__name__, dir=temp_subdirectory_1_1)
        _, temp_regular_file_2_4 = mkstemp(prefix='A' + self.__class__.__name__, dir=temp_subdirectory_1_1)

        reference_list = [
            temp_regular_file_0_1,
            temp_regular_file_1_3, temp_regular_file_1_4,
            temp_regular_file_1_2, temp_regular_file_1_1,
            temp_regular_file_2_4, temp_regular_file_2_3, temp_regular_file_2_2, temp_regular_file_2_1,
            temp_regular_file_1_6, temp_regular_file_1_5
        ]

        dir_entries = list(list_regular_files(self.temp_dir, recursive=True))
        self.assertListEqual(dir_entries, reference_list)

        dir_entries_unicode = list(list_regular_files(self.temp_dir, recursive=True))
        self.assertListEqual(dir_entries_unicode, reference_list)

    def test_find_file_valid(self):
        true_file = os.path.join(TESTDATA_DIR, 'test_frictionless.resource.yaml')
        match_file = find_file(TESTDATA_DIR, '(.*)frictionless\\.resource(.*)\\.yaml')
        self.assertEqual(true_file, match_file)

    def test_find_file_invalid(self):
        match_file = find_file(TESTDATA_DIR, '(.*)not\\.a\\.real\\.regex(.*)\\.yaml')
        self.assertIsNone(match_file)

    def test_mkdir_p(self):
        temp_dir = os.path.join(self.temp_dir, 'a', 'b', 'c', 'd', 'e')
        mkdir_p(temp_dir)
        self.assertTrue(os.path.isdir(temp_dir))

        # should not raise if directory already exists
        with self.assertNoException():
            mkdir_p(temp_dir)

        # test failure due to insufficient permissions
        os.chmod(temp_dir, 0o400)
        temp_dir_child = os.path.join(temp_dir, 'f')
        with self.assertRaises(OSError):
            mkdir_p(temp_dir_child)

    def test_rm_f(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        with self.assertNoException():
            rm_f(get_nonexistent_path())

        rm_f(temp_file)
        self.assertFalse(os.path.exists(temp_file))

        # [Errno 1] or [Errno 21] depeding on the OS...
        with self.assertRaises(OSError):
            rm_f(temp_dir)

    def test_rm_r(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        with self.assertRaisesRegex(OSError, r'\[Errno 2\].*'):
            rm_r(get_nonexistent_path())

        with self.assertNoException():
            rm_r(temp_file)

        with self.assertNoException():
            rm_r(temp_dir)

    def test_rm_rf(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        with self.assertNoException():
            rm_rf(get_nonexistent_path())

        with self.assertNoException():
            rm_rf(temp_file)

        with self.assertNoException():
            rm_rf(temp_dir)

    def test_safe_copy_file(self):
        nonexistent_file = get_nonexistent_path()
        with self.assertRaisesRegex(OSError, r'source file .* does not exist'):
            safe_copy_file(nonexistent_file, os.path.join(self.temp_dir, nonexistent_file))

        temp_source_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))
        temp_dest_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))

        with open(temp_source_file_path, 'w') as f:
            f.write(u'foobar')

        with self.assertRaisesRegex(OSError, r"source file and destination file can't refer the to same file"):
            safe_copy_file(temp_source_file_path, temp_source_file_path)

        safe_copy_file(temp_source_file_path, temp_dest_file_path)
        self.assertTrue(filecmp.cmp(temp_source_file_path, temp_dest_file_path, shallow=False))

        with self.assertRaisesRegex(OSError, r'destination file .* already exists'):
            safe_copy_file(temp_source_file_path, temp_dest_file_path)

        safe_copy_file(temp_source_file_path, temp_dest_file_path, overwrite=True)
        self.assertTrue(filecmp.cmp(temp_source_file_path, temp_dest_file_path, shallow=False))

    def test_safe_move_file(self):
        _, temp_source_file_path = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dest_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))

        safe_move_file(temp_source_file_path, temp_dest_file_path)
        self.assertTrue(os.path.exists(temp_dest_file_path))
        self.assertFalse(os.path.exists(temp_source_file_path))

    def test_get_file_checksum(self):
        temp_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))

        with open(temp_file_path, 'w') as f:
            f.write(u'foobar')

        expected_checksum = 'c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2'
        actual_checksum = get_file_checksum(temp_file_path)
        self.assertEqual(expected_checksum, actual_checksum)

    def test_temporary_directory(self):
        with TemporaryDirectory() as d:
            self.assertTrue(os.path.isdir(d))
            try:
                _, temp_file_path = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=d)
                with open(temp_file_path, 'w') as f:
                    f.write(u'foobar')
            except Exception as e:
                raise AssertionError(
                    "temporary directory is not writable. {e}".format(e=format_exception(e)))
            self.assertTrue(os.path.isfile(temp_file_path))
        self.assertFalse(os.path.exists(d))
