import filecmp
import os
import socket
import uuid
import zipfile
from tempfile import mkdtemp, mkstemp

import netCDF4
import six

from aodncore.util import (extract_zip, is_netcdffile, is_zipfile, list_regular_files, mkdir_p,
                           rm_f, rm_r, rm_rf, safe_copy_file, safe_move_file, get_file_checksum, TemporaryDirectory)
from aodncore.util.misc import format_exception
from test_aodncore.testlib import BaseTestCase, get_nonexistent_path

StringIO = six.StringIO

TEST_ROOT = os.path.join(os.path.dirname(__file__))
GOOD_NC = os.path.join(TEST_ROOT, 'good.nc')


class TestUtilFileOps(BaseTestCase):
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

    def test_isnetcdffile(self):
        _, temp_other_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        with open(temp_other_file, 'w') as f:
            f.write('foobar')

        self.assertTrue(is_netcdffile(self.temp_nc_file))
        self.assertFalse(is_netcdffile(temp_other_file))

    def test_iszipfile(self):
        temp_file_name = str(uuid.uuid4())
        temp_file_content = str(uuid.uuid4())

        _, temp_zip_file = mkstemp(suffix='.zip', prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_other_file = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=self.temp_dir)
        with zipfile.ZipFile(temp_zip_file, 'w', zipfile.ZIP_DEFLATED) as z:
            z.writestr(temp_file_name, temp_file_content)
        with open(temp_other_file, 'w') as f:
            f.write('foobar')

        self.assertTrue(is_zipfile(temp_zip_file))
        self.assertFalse(is_zipfile(temp_other_file))

    def test_list_regular_files(self):
        # regular file
        _, temp_regular_file1 = mkstemp(prefix='b' + self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file2 = mkstemp(prefix='a' + self.__class__.__name__, dir=self.temp_dir)

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

        dir_entries = list(list_regular_files(self.temp_dir))
        self.assertListEqual(dir_entries, [temp_regular_file2, temp_regular_file1])

        with self.assertRaises(ValueError):
            list_regular_files(self.temp_dir, sort_key='non_callable_key')

    def test_list_regular_files_recursive(self):
        _, temp_regular_file1 = mkstemp(prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_subdirectory1 = mkdtemp(prefix='b' + self.__class__.__name__, dir=self.temp_dir)
        temp_subdirectory2 = mkdtemp(prefix='a' + self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file2 = mkstemp(prefix=self.__class__.__name__, dir=temp_subdirectory1)
        _, temp_regular_file3 = mkstemp(prefix=self.__class__.__name__, dir=temp_subdirectory2)
        temp_subdirectory4 = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)
        _, temp_regular_file4 = mkstemp(prefix='b' + self.__class__.__name__, dir=temp_subdirectory4)
        _, temp_regular_file5 = mkstemp(prefix='a' + self.__class__.__name__, dir=temp_subdirectory4)

        dir_entries = list(list_regular_files(self.temp_dir, recursive=True))
        self.assertListEqual(dir_entries, [temp_regular_file1, temp_regular_file3, temp_regular_file2,
                                           temp_regular_file5, temp_regular_file4])

    def test_mkdir_p(self):
        temp_dir = os.path.join(self.temp_dir, 'a', 'b', 'c', 'd', 'e')
        mkdir_p(temp_dir)
        self.assertTrue(os.path.isdir(temp_dir))

        # should not raise if directory already exists
        try:
            mkdir_p(temp_dir)
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))

        # test failure due to insufficient permissions
        os.chmod(temp_dir, 0o400)
        temp_dir_child = os.path.join(temp_dir, 'f')
        with self.assertRaises(OSError):
            mkdir_p(temp_dir_child)

    def test_rm_f(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        try:
            rm_f(get_nonexistent_path())
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        rm_f(temp_file)
        self.assertFalse(os.path.exists(temp_file))

        with self.assertRaisesRegexp(OSError, '[Errno 21].*'):
            rm_f(temp_dir)

    def test_rm_r(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        with self.assertRaisesRegexp(OSError, '[Errno 2].*'):
            rm_r(get_nonexistent_path())

        try:
            rm_r(temp_file)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        try:
            rm_r(temp_dir)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

    def test_rm_rf(self):
        _, temp_file = mkstemp(suffix='.tmp', prefix=self.__class__.__name__, dir=self.temp_dir)
        temp_dir = mkdtemp(prefix=self.__class__.__name__, dir=self.temp_dir)

        try:
            rm_rf(get_nonexistent_path())
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        try:
            rm_rf(temp_file)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

        try:
            rm_rf(temp_dir)
        except Exception as e:
            raise AssertionError("unexpected exception raised. {e}".format(e=format_exception(e)))

    def test_safe_copy_file(self):
        nonexistent_file = get_nonexistent_path()
        with self.assertRaisesRegexp(OSError, "source file .* does not exist"):
            safe_copy_file(nonexistent_file, os.path.join(self.temp_dir, nonexistent_file))

        temp_source_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))
        temp_dest_file_path = os.path.join(self.temp_dir, str(uuid.uuid4()))

        with open(temp_source_file_path, 'w') as f:
            f.write('foobar')

        with self.assertRaisesRegexp(OSError, "source file and destination file can't refer the to same file"):
            safe_copy_file(temp_source_file_path, temp_source_file_path)

        safe_copy_file(temp_source_file_path, temp_dest_file_path)
        self.assertTrue(filecmp.cmp(temp_source_file_path, temp_dest_file_path, shallow=False))

        with self.assertRaisesRegexp(OSError, "destination file .* already exists"):
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
            f.write('foobar')

        expected_checksum = 'c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2'
        actual_checksum = get_file_checksum(temp_file_path)
        self.assertEqual(expected_checksum, actual_checksum)

    def test_temporary_directory(self):
        with TemporaryDirectory() as d:
            self.assertTrue(os.path.isdir(d))
            try:
                _, temp_file_path = mkstemp(suffix='.txt', prefix=self.__class__.__name__, dir=d)
                with open(temp_file_path, 'w') as f:
                    f.write('foobar')
            except Exception as e:
                raise AssertionError(
                    "temporary directory is not writable. {e}".format(e=format_exception(e)))
            self.assertTrue(os.path.isfile(temp_file_path))
        self.assertFalse(os.path.exists(d))
