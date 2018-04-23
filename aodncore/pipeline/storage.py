import abc
import errno
import os

import boto3
from botocore.exceptions import ClientError
from paramiko import SSHClient, AutoAddPolicy
from six.moves.urllib.parse import urlparse

from .exceptions import AttributeNotSetError, FileDeleteFailedError, FileUploadFailedError, InvalidStoreUrlError
from .files import validate_pipelinefilecollection
from ..util import format_exception, mkdir_p, retry_decorator, rm_f, safe_copy_file

__all__ = [
    'get_storage_broker',
    'LocalFileStorageBroker',
    'S3StorageBroker',
    'SftpStorageBroker',
    'sftp_makedirs',
    'sftp_mkdir_p',
    'sftp_path_exists'
]


def get_storage_broker(store_url, config, logger):
    """Factory function to return appropriate storage broker class based on URL scheme

    :param store_url: URL base
    :param config: LazyConfigManager instance
    :param logger: Logger instance
    :return: BaseStorageBroker sub-class
    """

    url = urlparse(store_url)
    if url.scheme == 'file':
        return LocalFileStorageBroker(url.path, config, logger)
    elif url.scheme == 's3':
        return S3StorageBroker(url.netloc, url.path, config, logger)
    elif url.scheme == 'sftp':
        return SftpStorageBroker(url.netloc, url.path, config, logger)
    else:
        raise InvalidStoreUrlError("invalid URL scheme '{scheme}'".format(scheme=url.scheme))


class BaseStorageBroker(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, logger):
        self._config = config
        self._logger = logger
        self.prefix = None

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
    def _upload_file(self, pipeline_file, dest_path_attr):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _get_absolute_dest_uri(self, pipeline_file, dest_path_attr):
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
        validate_pipelinefilecollection(pipeline_files)

        should_upload = pipeline_files.filter_by_bool_attributes_and_not('should_store', 'is_deletion')
        for pipeline_file in should_upload:
            abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            pipeline_file.is_overwrite = self._get_is_overwrite(pipeline_file, abs_path)

    def upload(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        validate_pipelinefilecollection(pipeline_files)

        self._pre_run_hook()

        for pipeline_file in pipeline_files:
            self._logger.info(
                "uploading '{source_path}' to '{uri}'".format(source_path=pipeline_file.src_path,
                                                              uri=self._get_absolute_dest_uri(
                                                                  pipeline_file=pipeline_file,
                                                                  dest_path_attr=dest_path_attr)))
            try:
                self._upload_file(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            except Exception as e:
                raise FileUploadFailedError(
                    "{e}: '{dest_path}'".format(e=format_exception(e),
                                                dest_path=getattr(pipeline_file, dest_path_attr)))
            setattr(pipeline_file, is_stored_attr, True)

        self._post_run_hook()

    def delete(self, pipeline_files, is_stored_attr='is_stored', dest_path_attr='dest_path'):
        validate_pipelinefilecollection(pipeline_files)

        self._pre_run_hook()

        for pipeline_file in pipeline_files:
            self._logger.info("deleting '{uri}'".format(
                uri=self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)))
            try:
                self._delete_file(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
            except Exception as e:
                raise FileDeleteFailedError(
                    "{e}: '{dest_path}'".format(e=format_exception(e),
                                                dest_path=getattr(pipeline_file, dest_path_attr)))
            setattr(pipeline_file, is_stored_attr, True)

        self._post_run_hook()


class LocalFileStorageBroker(BaseStorageBroker):
    """StorageBroker to interact with a local directory
    """

    def __init__(self, prefix, config, logger):
        super(LocalFileStorageBroker, self).__init__(config, logger)
        self.prefix = prefix

    def _delete_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        rm_f(abs_path)

    def _get_absolute_dest_uri(self, pipeline_file, dest_path_attr):
        return "file://{path}".format(
            path=self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr))

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return os.path.exists(abs_path)

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        return

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

    def __init__(self, bucket, prefix, config, logger):
        super(S3StorageBroker, self).__init__(config, logger)

        self.bucket = bucket
        self.prefix = prefix

        self.s3_client = boto3.client('s3')

    @retry_decorator(**retry_kwargs)
    def _delete_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file=pipeline_file, dest_path_attr=dest_path_attr)
        self.s3_client.delete_object(Bucket=self.bucket, Key=abs_path)

    def _get_absolute_dest_uri(self, pipeline_file, dest_path_attr):
        return "s3://{bucket}/{path}".format(bucket=self.bucket,
                                             path=self._get_absolute_dest_path(pipeline_file=pipeline_file,
                                                                               dest_path_attr=dest_path_attr))

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

    def __init__(self, server, prefix, config, logger):
        super(SftpStorageBroker, self).__init__(config, logger)
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

    def _get_absolute_dest_uri(self, pipeline_file, dest_path_attr):
        return "sftp://{server}{path}".format(server=self.server,
                                              path=self._get_absolute_dest_path(pipeline_file=pipeline_file,
                                                                                dest_path_attr=dest_path_attr))

    def _get_is_overwrite(self, pipeline_file, abs_path):
        return sftp_path_exists(self.sftp_client, abs_path)

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        self._connect_sftp()

    def _upload_file(self, pipeline_file, dest_path_attr):
        abs_path = self._get_absolute_dest_path(pipeline_file, dest_path_attr=dest_path_attr)
        parent_dir = os.path.dirname(abs_path)
        sftp_mkdir_p(self.sftp_client, parent_dir)

        with open(pipeline_file.src_path, 'rb') as f:
            self.sftp_client.putfo(f, abs_path, confirm=True)
