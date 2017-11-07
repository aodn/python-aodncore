import errno
import hashlib
import os
import shutil
import tempfile
import zipfile
from functools import partial

import netCDF4
import six

try:
    from os import scandir, walk
except ImportError:
    from scandir import scandir, walk

StringIO = six.StringIO

__all__ = [
    'extract_zip',
    'is_netcdffile',
    'is_zipfile',
    'list_regular_files',
    'mkdir_p',
    'rm_f',
    'rm_r',
    'rm_rf',
    'safe_copy_file',
    'safe_move_file',
    'get_file_checksum',
    'TemporaryDirectory'
]


def extract_zip(zip_path, dest_dir):
    """Extract a ZIP file's contents into a directory
    
    :param zip_path: path to the source ZIP file
    :param dest_dir: destination directory into which the ZIP is extracted
    :return: None
    """
    with zipfile.ZipFile(zip_path, mode='r') as z:
        z.extractall(dest_dir)


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


def is_zipfile(filepath):
    """Check whether a file path refers to a valid ZIP file

    :param filepath: path to the file being checked
    :return: True if filepath is a valid ZIP file, otherwise False 
    """
    return zipfile.is_zipfile(filepath)


def list_regular_files(path, recursive=False, sort_key=str.lower):
    """List all regular files in a given directory, returning the absolute path

    :param sort_key: callable used to sort directory listings
    :param path: input directory to list
    :param recursive: boolean flag to enable recursive listing
    :return: iterator returning only regular files
    """
    if not callable(sort_key):
        raise ValueError("sort_key must be callable")

    def nonrecursive_list(path_, sort_key_=sort_key):
        dir_entries = sorted(scandir(os.path.abspath(path_)), key=lambda p: sort_key_(p.name))
        return (f.path for f in dir_entries if f.is_file(follow_symlinks=False))

    def recursive_list(path_, sort_key_=sort_key):
        for root, dirs, files in walk(path_):
            dirs.sort(key=sort_key_)
            files.sort(key=sort_key_)
            for name in files:
                fullpath = os.path.join(root, name)
                if not os.path.islink(fullpath):
                    yield os.path.abspath(fullpath)

    inner_func = recursive_list if recursive else nonrecursive_list
    return inner_func(path, sort_key)


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
        rm_f(temp_destination_name)


def safe_move_file(src, dst, overwrite=False):
    """Move a file atomically by performing a copy and delete

    :param src: source file path
    :param dst: destination file path
    :param overwrite: set to True to allow existing destination file to be overwritten
    :return: None
    """
    safe_copy_file(src, dst, overwrite)
    os.remove(src)


def get_file_checksum(filepath, block_size=65536, algorithm='sha256'):
    """Get the hash of a file

    :param filepath: path to the input file
    :param block_size: number of bytes to hash each iteration
    :param algorithm: hash algorithm (from hashlib module)
    :return: hash of the input file
    """
    hash_function = getattr(hashlib, algorithm)
    hasher = hash_function()
    with open(filepath, 'rb') as f:
        for block in iter(partial(f.read, block_size), b''):
            hasher.update(block)
    return hasher.hexdigest()


class _TemporaryDirectory(object):
    """Context manager for tempfile.mkdtemp() (available in core library in v3.2+).
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
