"""This module provides utility functions relating to various filesystem operations
"""

import errno
import gzip
import hashlib
import locale
import os
import shutil
import tempfile
import zipfile
from functools import cmp_to_key, partial
from tempfile import TemporaryFile

import netCDF4
import six

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

StringIO = six.StringIO

locale.setlocale(locale.LC_ALL, 'C')

__all__ = [
    'TemporaryDirectory',
    'extract_gzip',
    'extract_zip',
    'get_file_checksum',
    'is_dir_writable',
    'is_file_writable',
    'is_gzipfile',
    'is_netcdffile',
    'is_nonemptyfile',
    'is_zipfile',
    'list_regular_files',
    'mkdir_p',
    'rm_f',
    'rm_r',
    'rm_rf',
    'safe_copy_file',
    'safe_move_file',
    'validate_dir_writable',
    'validate_file_writable'
]


class _TemporaryDirectory(object):
    """Context manager for :py:function:`tempfile.mkdtemp` (available in core library in v3.2+).
    """

    def __init__(self, suffix="", prefix=None, dir=None):
        self._closed = False
        self.name = None

        dir_prefix = prefix if prefix else self.__class__.__name__
        self.name = tempfile.mkdtemp(suffix=suffix, prefix=dir_prefix, dir=dir)

        self._rmtree = shutil.rmtree

    def __del__(self):
        self.cleanup()

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        self.cleanup()

    def __repr__(self):  # pragma: no cover
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def cleanup(self):
        try:
            self._rmtree(self.name)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass  # pragma: no cover


try:
    TemporaryDirectory = tempfile.TemporaryDirectory
except AttributeError:
    TemporaryDirectory = _TemporaryDirectory


def extract_gzip(gzip_path, dest_dir, dest_name=None):
    """Extract a GZ (GZIP) file's contents into a directory

    :param gzip_path: path to the source GZ file
    :param dest_dir: destination directory into which the GZ is extracted
    :param dest_name: basename for the extracted file (defaults to the original name minus the '.gz' extension)
    :return: None
    """
    if dest_name is None:
        dest_name = os.path.basename(gzip_path).rstrip('.gz')

    dest_path = os.path.join(dest_dir, dest_name)
    with open(dest_path, 'wb') as f, gzip.open(gzip_path) as g:
            shutil.copyfileobj(g, f)


def extract_zip(zip_path, dest_dir):
    """Extract a ZIP file's contents into a directory
    
    :param zip_path: path to the source ZIP file
    :param dest_dir: destination directory into which the ZIP is extracted
    :return: None
    """
    with zipfile.ZipFile(zip_path, mode='r') as z:
        z.extractall(dest_dir)


def get_file_checksum(filepath, block_size=65536, algorithm='sha256'):
    """Get the hash (checksum) of a file

    :param filepath: path to the input file
    :param block_size: number of bytes to hash each iteration
    :param algorithm: hash algorithm (from :py:mod:`hashlib` module)
    :return: hash of the input file
    """
    hash_function = getattr(hashlib, algorithm)
    hasher = hash_function()
    with open(filepath, 'rb') as f:
        for block in iter(partial(f.read, block_size), b''):
            hasher.update(block)
    return hasher.hexdigest()


def is_dir_writable(path):
    """Check whether a directory is writable

    :param path: directory path to check
    :return: None
    """
    try:
        with TemporaryFile(prefix='is_dir_writable', suffix='.tmp', dir=path) as t:
            t.write('is_dir_writable')
    except IOError as e:
        if e.errno == errno.EACCES:
            return False
    else:
        return True


def is_file_writable(path):
    """Check whether a file is writable

        .. note:: Not as reliable as the :py:func:`is_dir_writable` function since that actually writes a file

    :param path: file path to check
    :return: None
    """
    return os.access(path, os.W_OK)


def is_gzipfile(filepath):
    """Check whether a file path refers to a valid ZIP file

    :param filepath: path to the file being checked
    :return: True if filepath is a valid ZIP file, otherwise False
    """
    try:
        with gzip.open(filepath) as g:
            _ = g.read(1)
        return True
    except IOError:
        return False


def is_netcdffile(filepath):
    """Check whether a file path refers to a valid NetCDF file

    :param filepath: path to the file being checked
    :return: True if filepath is a valid NetCDF file, otherwise False 
    """
    fh = None
    try:
        fh = netCDF4.Dataset(filepath, mode='r')
    except IOError:
        return False
    else:
        return True
    finally:
        if fh:
            fh.close()


def is_nonemptyfile(filepath):
    """Check whether a file path refers to a file with length greater than zero

    :param filepath: path to the file being checked
    :return: True if filepath is non-zero, otherwise False
    """
    return os.path.getsize(filepath) > 0


def is_zipfile(filepath):
    """Check whether a file path refers to a valid ZIP file

    :param filepath: path to the file being checked
    :return: True if filepath is a valid ZIP file, otherwise False 
    """
    return zipfile.is_zipfile(filepath)


def list_regular_files(path, recursive=False, sort_key=cmp_to_key(locale.strcoll)):
    """List all regular files in a given directory, returning the absolute path

    :param sort_key: callable used to sort directory listings
    :param path: input directory to list
    :param recursive: :py:class:`bool` flag to enable recursive listing
    :return: iterator returning only regular files
    """
    if not callable(sort_key):
        raise ValueError("sort_key must be callable")

    def nonrecursive_list(path_):
        dir_entries = sorted(scandir(os.path.abspath(path_)), key=lambda p: sort_key(p.name))
        return (f.path for f in dir_entries if f.is_file(follow_symlinks=False))

    def recursive_list(path_):
        for root, dirs, files in walk(path_):
            dirs.sort(key=sort_key)
            files.sort(key=sort_key)
            for name in files:
                fullpath = os.path.join(root, name)
                if not os.path.islink(fullpath):
                    yield os.path.abspath(fullpath)

    inner_func = recursive_list if recursive else nonrecursive_list
    return inner_func(path)


def mkdir_p(path, mode=0o755):
    """Recursively create a directory, including parent directories (analogous to shell command 'mkdir -p')

    :param mode:
    :param path: path to new directory
    :return: None
    """
    try:
        os.makedirs(path, mode)
    except OSError as e:
        if e.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def rm_f(path):
    """Remove a file, ignoring "file not found" errors (analogous to shell command 'rm -f')
    
    :param path: path to file being deleted
    :return: None
    """
    try:
        os.remove(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise  # pragma: no cover


def rm_r(path):
    """Remove a file or directory recursively (analogous to shell command 'rm -r')

    :param path: path to file being deleted
    :return: None
    """
    try:
        shutil.rmtree(path)
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            os.remove(path)
        else:
            raise


def rm_rf(path):
    """Remove a file or directory, ignoring "file not found" errors (analogous to shell command 'rm -f')

    :param path: path to file being deleted
    :return: None
    """
    try:
        shutil.rmtree(path)
    except OSError as e:
        if e.errno == errno.ENOTDIR:
            rm_f(path)
        elif e.errno != errno.ENOENT:
            raise  # pragma: no cover


def safe_copy_file(source, destination, overwrite=False):
    """Copy a file atomically by copying first to a temporary file in the same directory (and therefore filesystem) as
    the intended destination, before performing a rename (which is atomic)

    :param source: source file path
    :param destination: destination file path (will not be overwritten unless 'overwrite' set to True)
    :param overwrite: set to True to allow existing destination file to be overwritten
    :return: None
    """
    if not os.path.exists(source):
        raise OSError("source file '{source}' does not exist".format(source=source))
    if source == destination:
        raise OSError("source file and destination file can't refer the to same file")
    if not overwrite and os.path.exists(destination):
        raise OSError("destination file '{destination}' already exists".format(destination=destination))

    temp_destination_name = None
    try:
        with tempfile.NamedTemporaryFile(mode='wb', dir=os.path.dirname(destination), delete=False) as temp_destination:
            temp_destination_name = temp_destination.name
            with open(source, 'rb') as f:
                shutil.copyfileobj(f, temp_destination)
        os.rename(temp_destination_name, destination)
    finally:
        try:
            if temp_destination_name:
                rm_f(temp_destination_name)
        except OSError as e:  # pragma: no cover
            if e.errno != errno.ENOENT:
                raise


def safe_move_file(src, dst, overwrite=False):
    """Move a file atomically by performing a copy and delete

    :param src: source file path
    :param dst: destination file path
    :param overwrite: set to True to allow existing destination file to be overwritten
    :return: None
    """
    safe_copy_file(src, dst, overwrite)
    os.remove(src)


def validate_dir_writable(path):
    if not is_dir_writable(path):
        raise ValueError("dir '{dir}' is not writable".format(dir=path))


def validate_file_writable(path):
    if not is_file_writable(path):
        raise ValueError("file '{file}' is not writable".format(file=path))
