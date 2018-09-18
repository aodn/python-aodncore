import abc
import errno
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from paramiko import SSHClient, AutoAddPolicy
from six.moves.urllib.parse import urlparse

try:
    from os import walk
except ImportError:
    from scandir import walk

from .exceptions import AttributeNotSetError, InvalidStoreUrlError, StorageBrokerError
from .files import ensure_pipelinefilecollection, PipelineFile, PipelineFileCollection
from ..util import (ensure_regex_list, format_exception, matches_regexes, mkdir_p, retry_decorator, rm_f,
                    safe_copy_file, validate_relative_path, validate_type)

__all__ = [
    'get_storage_broker',
    'LocalFileStorageBroker',
    'S3StorageBroker',
    'SftpStorageBroker',
    'sftp_makedirs',
    'sftp_mkdir_p',
    'sftp_path_exists',
    'validate_storage_broker'
]

DISALLOWED_DELETE_REGEXES = {'', '.*', '.+'}


def get_storage_broker(store_url):
    """Factory function to return appropriate storage broker class based on URL scheme

    :param store_url: URL base
    :return: BaseStorageBroker sub-class
    """

    url = urlparse(store_url)
    if url.scheme == 'file':
        if url.netloc:
            raise InvalidStoreUrlError("invalid URL '{url}'. Must be an absolute path".format(url=store_url))
        return LocalFileStorageBroker(url.path)
    elif url.scheme == 's3':
        return S3StorageBroker(url.netloc, url.path)
    elif url.scheme == 'sftp':
        return SftpStorageBroker(url.netloc, url.path)
    else:
        raise InvalidStoreUrlError("invalid URL scheme '{scheme}'".format(scheme=url.scheme))


class BaseStorageBroker(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.prefix = None

    def __repr__(self):
        return "{name}(prefix='{prefix}')".format(name=self.__class__.__name__, prefix=self.prefix)

    @abc.abstractmethod  # pragma: no cover
    def _delete_file(self, pipeline_file, dest_path_attr):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _post_run_hook(self):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _pre_run_hook(self):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _run_query(self, query):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _upload_file(self, pipeline_file, dest_path_attr):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _get_is_overwrite(self, pipeline_file, abs_path):
        pass

    def _get_absolute_dest_path(self, pipeline_file, dest_path_attr):
        rel_path = getattr(pipeline_file, dest_path_attr)
        if not rel_path:
            raise AttributeNotSetError(
                "attribute '{attr}' not set in '{pf}'".format(attr=dest_path_attr, pf=pipeline_file))
        return os.path.join(self.prefix, rel_path)

    def set_is_overwrite(self, pipeline_files, dest_path_attr='dest_path'):
        overwrite_collection = ensure_pipelinefilecollection(pipeline_files)

        should_upload = overwrite_collection.filter_by_bool_attributes_and_not('should_store', 'is_deletion')
        for pipeline_file in should_upload:
            abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            pipeline_file.is_overwrite = self._get_is_overwrite(pipeline_file, abs_path)

    def upload(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        """Upload the given PipelineFileCollection or PipelineFile to the storage backend

        :param pipeline_files: collection to upload
        :param is_stored_attr: PipelineFile attribute which will be set to True if upload is successful
        :param dest_path_attr: PipelineFile attribute containing the destination path
        :return: None
        """
        upload_collection = ensure_pipelinefilecollection(pipeline_files)

        self._pre_run_hook()

        for pipeline_file in upload_collection:
            try:
                self._upload_file(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            except Exception as e:
                raise StorageBrokerError("error uploading '{dest_path}': {e}".format(
                    dest_path=getattr(pipeline_file, dest_path_attr), e=format_exception(e)))

            setattr(pipeline_file, is_stored_attr, True)

        self._post_run_hook()

    def delete(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        """Delete the given PipelineFileCollection or PipelineFile from the storage backend

        :param pipeline_files: collection to delete
        :param is_stored_attr: PipelineFile attribute which will be set to True if delete is successful
        :param dest_path_attr: PipelineFile attribute containing the destination path
        :return: None
        """
        delete_collection = ensure_pipelinefilecollection(pipeline_files)

        self._pre_run_hook()

        for pipeline_file in delete_collection:
            try:
                self._delete_file(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            except Exception as e:
                raise StorageBrokerError("error deleting '{dest_path}': {e}".format(
                    dest_path=getattr(pipeline_file, dest_path_attr), e=format_exception(e)))

            setattr(pipeline_file, is_stored_attr, True)

        self._post_run_hook()

    def delete_regexes(self, regexes, allow_match_all=False):
        """Delete files storage if they match one of the given regular expressions

        :param regexes: list of regular expressions to delete
        :param allow_match_all: boolean flag controlling whether "match all" regexes are accepted
        :return: PipelineFileCollection of files which matched the patterns and were deleted
        """
        delete_regexes = ensure_regex_list(regexes)

        delete_regex_strings = {r.pattern for r in delete_regexes}
        if delete_regex_strings.intersection(DISALLOWED_DELETE_REGEXES) and not allow_match_all:
            raise ValueError("regexes '{disallowed}' disallowed unless allow_match_all=True".format(
                disallowed=list(DISALLOWED_DELETE_REGEXES)))

        files_to_delete = PipelineFileCollection()
        if not delete_regexes:
            return files_to_delete

        all_files = self.query()
        files_to_delete.update(
            PipelineFile(l, dest_path=os.path.join(self.prefix, l), is_deletion=True)
            for l in all_files
            if matches_regexes(l, delete_regexes)
        )

        self.delete(files_to_delete)
        return files_to_delete

    def query(self, query=''):
        """Query the storage for existing files

        A trailing slash will result in a directory listing type of query, recursively listing all files underneath
        the given directory.

        Omitting the trailing slash will cause a prefix style query, where the results will be any path that matches the
        query *including* partial file names.

        :param query: S3 prefix style string (if omitted, will search with a blank prefix)
        :return: a dict with keys being the matching objects, and values being a metadata dict for the object
        """
        try:
            return self._run_query(query)
        except Exception as e:
            raise StorageBrokerError("error querying storage: {e}".format(query=query, e=format_exception(e)))


class LocalFileStorageBroker(BaseStorageBroker):
    """StorageBroker to interact with a local directory
    """

    def __init__(self, prefix):
        super(LocalFileStorageBroker, self).__init__()
        self.prefix = prefix

    def _delete_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        rm_f(abs_path)

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return os.path.exists(abs_path)

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        return

    def _run_query(self, query):
        validate_relative_path(query)

        full_query = os.path.join(self.prefix, query)

        def _find_prefix(path):
            parent_path = os.path.dirname(path)
            for root, dirs, files in walk(parent_path):
                for name in files:
                    fullpath = os.path.join(root, name)
                    if fullpath.startswith(full_query) and not os.path.islink(fullpath):
                        stats = os.stat(fullpath)
                        key = os.path.relpath(fullpath, self.prefix)
                        yield key, {'last_modified': datetime.fromtimestamp(stats.st_mtime), 'size': stats.st_size}

        result = dict(_find_prefix(full_query))
        return result

    def _upload_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        mkdir_p(os.path.dirname(abs_path))
        safe_copy_file(pipeline_file.src_path, abs_path, overwrite=True)


class S3StorageBroker(BaseStorageBroker):
    """StorageBroker to interact with an S3

    Note: this does not and should not attempt to support any authentication code. Multiple mechanisms for loading
            credentials are far more appropriately handled directly by the boto3, and it is expected that the
            credentials are supplied using one of these mechanisms by the environment (e.g. deployed from configuration
            management, set as environment variables etc.)

            Refer: http://boto3.readthedocs.io/en/latest/guide/configuration.html

    """

    retry_kwargs = {
        'tries': 3,
        'delay': 5,
        'backoff': 2,
        'exceptions': (ClientError,)
    }

    def __init__(self, bucket, prefix):
        super(S3StorageBroker, self).__init__()

        self.bucket = bucket
        self.prefix = prefix

        self.s3_client = boto3.client('s3')

    @retry_decorator(**retry_kwargs)
    def _delete_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        self.s3_client.delete_object(Bucket=self.bucket, Key=abs_path)

    @retry_decorator(**retry_kwargs)
    def _get_is_overwrite(self, pipeline_file, abs_path):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=abs_path)
        return bool([k for k in response.get('Contents', []) if k['Key'] == abs_path])

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        try:
            self._validate_bucket()
        except Exception as e:
            raise InvalidStoreUrlError(
                "unable to access S3 bucket '{0}': {1}".format(self.bucket, format_exception(e)))

    def _run_query(self, query):
        full_query = os.path.join(self.prefix, query)
        raw_result = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=full_query)
        result = {k['Key']: {'last_modified': k['LastModified'], 'size': k['Size']}
                  for k in raw_result.get('Contents', [])}
        return result

    @retry_decorator(**retry_kwargs)
    def _upload_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)

        with open(pipeline_file.src_path, 'rb') as f:
            self.s3_client.upload_fileobj(f, Bucket=self.bucket, Key=abs_path,
                                          ExtraArgs={'ContentType': pipeline_file.mime_type})

    @retry_decorator(**retry_kwargs)
    def _validate_bucket(self):
        self.s3_client.head_bucket(Bucket=self.bucket)


def sftp_path_exists(sftpclient, path):
    """Test whether a path exists on a remote SFTP server

    :param sftpclient: SFTPClient object
    :param path: path to test for existence
    :return: True if the path exists, False if not
    """
    try:
        sftpclient.stat(path)
    except IOError as e:
        if e.errno == errno.ENOENT:
            return False
        raise
    return True


def sftp_makedirs(sftpclient, name, mode=0o755):
    """Recursively create a directory path on a remote SFTP server
        Based on os.makedirs, with local calls replaced with SFTPClient equivalents calls.

    :param sftpclient: SFTPClient object
    :param name: directory path to create
    :param mode: permissions for the newly created directory
    :return: None
    """
    head, tail = os.path.split(name)
    if not tail:
        head, tail = os.path.split(head)

    if head and tail and not sftp_path_exists(sftpclient, head):
        try:
            sftp_makedirs(sftpclient, head, mode)
        except IOError as e:  # pragma: no cover
            if e.errno != errno.EEXIST:
                raise
        if tail == os.path.curdir:
            return

    sftpclient.mkdir(name, mode)


def sftp_mkdir_p(sftpclient, name, mode=0o755):
    """Replicate 'mkdir -p' shell command behaviour by wrapping sftp_makedirs and suppressing exceptions where the
        directory already exists.

    :param sftpclient: SFTPClient object
    :param name: directory path to create
    :param mode: permissions for the newly created directory
    :return: None
    """
    try:
        sftp_makedirs(sftpclient, name, mode)
    except IOError:
        if not sftp_path_exists(sftpclient, name):
            raise


class SftpStorageBroker(BaseStorageBroker):
    """StorageBroker to interact with a directory on a remote SFTP server

    Note: similar to the S3 storage broker, this does not implement any authentication code, as this is better handled
    by the environment in the form of public key authentication
    """

    def __init__(self, server, prefix):
        super(SftpStorageBroker, self).__init__()
        self.server = server
        self.prefix = prefix

        self._sshclient = SSHClient()

        # TODO: replace with more sensible policy... predetermined keys?
        self._sshclient.set_missing_host_key_policy(AutoAddPolicy())

        self.sftp_client = None

    def _connect_sftp(self):
        self._sshclient.connect(self.server)
        self.sftp_client = self._sshclient.open_sftp()

    def _delete_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        self.sftp_client.remove(abs_path)

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return sftp_path_exists(self.sftp_client, abs_path)

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        self._connect_sftp()

    def _run_query(self, query):
        raise NotImplementedError

    def _upload_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file, dest_path_attr=dest_path_attr)
        parent_dir = os.path.dirname(abs_path)
        sftp_mkdir_p(self.sftp_client, parent_dir)

        with open(pipeline_file.src_path, 'rb') as f:
            self.sftp_client.putfo(f, abs_path, confirm=True)


validate_storage_broker = validate_type(BaseStorageBroker)
