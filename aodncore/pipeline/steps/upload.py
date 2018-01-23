import abc
import errno
import os

import boto3
from paramiko import SSHClient, AutoAddPolicy
from six.moves.urllib.parse import urlparse

from .basestep import AbstractCollectionStepRunner
from ..exceptions import FileDeleteFailedError, FileUploadFailedError, InvalidUploadUrlError
from ..files import validate_pipelinefilecollection
from ...util import format_exception, mkdir_p, rm_f, safe_copy_file

__all__ = [
    'get_upload_runner',
    'FileUploadRunner',
    'S3UploadRunner',
    'SftpUploadRunner'
]


def get_upload_runner(upload_base_url, config, logger, archive_mode=False):
    """Factory function to return appropriate uploader class based on URL scheme

    :param upload_base_url: URL base
    :param config: LazyConfigManager instance
    :param logger: Logger instance
    :param archive_mode: flag to indicate archive
    :return: BaseUploadRunner sub-class
    """

    url = urlparse(upload_base_url)
    if url.scheme == 'file':
        return FileUploadRunner(url.path, config, logger, archive_mode)
    elif url.scheme == 's3':
        return S3UploadRunner(url.netloc, url.path, config, logger, archive_mode)
    elif url.scheme == 'sftp':
        return SftpUploadRunner(url.netloc, url.path, config, logger, archive_mode)
    else:
        raise InvalidUploadUrlError("invalid URL scheme '{scheme}'".format(scheme=url.scheme))


class BaseUploadRunner(AbstractCollectionStepRunner):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, logger, archive_mode=False):
        super(BaseUploadRunner, self).__init__(config, logger)
        self.prefix = None
        self.archive_mode = archive_mode

    @property
    def is_stored_attr(self):
        """PipelineFile attribute to flag completion of upload operation

        :return: bool
        """
        return 'is_archived' if self.archive_mode else 'is_stored'

    @property
    def pending_addition_attr(self):
        return 'pending_archive' if self.archive_mode else 'pending_store_addition'

    @property
    def dest_path_attr(self):
        """PipelineFile attribute containing the destination path

        :return: bool
        """
        return 'archive_path' if self.archive_mode else 'dest_path'

    @abc.abstractmethod  # pragma: no cover
    def _delete_file(self, pipeline_file):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _post_run_hook(self):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _pre_run_hook(self):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _upload_file(self, pipeline_file):
        pass

    @abc.abstractmethod  # pragma: no cover
    def _get_absolute_dest_uri(self, pipeline_file):
        pass

    @abc.abstractmethod  # pragma: no cover
    def determine_overwrites(self, pipeline_files):
        pass

    def _get_absolute_dest_path(self, pipeline_file):
        rel_path = getattr(pipeline_file, self.dest_path_attr)
        return os.path.join(self.prefix, rel_path)

    def run(self, pipeline_files):
        validate_pipelinefilecollection(pipeline_files)

        additions = pipeline_files.filter_by_bool_attribute(self.pending_addition_attr)
        deletions = pipeline_files.filter_by_bool_attribute('pending_store_deletion')
        undo_deletions = pipeline_files.filter_by_bool_attribute('pending_undo')

        self._pre_run_hook()

        for pipeline_file in additions:
            self._logger.info(
                "uploading '{source_path}' to '{uri}'".format(source_path=pipeline_file.src_path,
                                                              uri=self._get_absolute_dest_uri(pipeline_file)))
            try:
                self._upload_file(pipeline_file)
            except Exception as e:
                raise FileUploadFailedError(
                    "{e}: '{dest_path}'".format(e=format_exception(e),
                                                dest_path=getattr(pipeline_file, self.dest_path_attr)))
            setattr(pipeline_file, self.is_stored_attr, True)

        for pipeline_file in deletions:
            self._logger.info("deleting '{uri}'".format(uri=self._get_absolute_dest_path(pipeline_file)))
            try:
                self._delete_file(pipeline_file)
            except Exception as e:
                raise FileDeleteFailedError(
                    "{e}: '{dest_path}'".format(e=format_exception(e),
                                                dest_path=getattr(pipeline_file, self.dest_path_attr)))
            pipeline_file.is_stored = True

        for pipeline_file in undo_deletions:
            self._logger.info("undoing upload '{uri}'".format(uri=self._get_absolute_dest_uri(pipeline_file)))
            try:
                self._delete_file(pipeline_file)
            except Exception as e:
                raise FileDeleteFailedError(
                    "{e}: '{dest_path}'".format(e=format_exception(e),
                                                dest_path=getattr(pipeline_file, self.dest_path_attr)))
            pipeline_file.is_upload_undone = True

        self._post_run_hook()


class FileUploadRunner(BaseUploadRunner):
    """UploadRunner to "upload" a file to a local directory

    """

    def __init__(self, prefix, config, logger, archive_mode=False):
        super(FileUploadRunner, self).__init__(config, logger, archive_mode)
        self.prefix = prefix

    def _delete_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)
        rm_f(abs_path)

    def _get_absolute_dest_uri(self, pipeline_file):
        return "file://{path}".format(path=self._get_absolute_dest_path(pipeline_file))

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        return

    def _upload_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)
        mkdir_p(os.path.dirname(abs_path))
        safe_copy_file(pipeline_file.src_path, abs_path, overwrite=True)

    def determine_overwrites(self, pipeline_files):
        validate_pipelinefilecollection(pipeline_files)

        for pipeline_file in pipeline_files:
            abs_path = self._get_absolute_dest_path(pipeline_file)
            pipeline_file.is_overwrite = os.path.exists(abs_path)


class S3UploadRunner(BaseUploadRunner):
    """UploadRunner to upload files to an S3 bucket

    Note: this does not and should not attempt to support any authentication code. Multiple mechanisms for loading
            credentials are far more appropriately handled directly by the boto3, and it is expected that the
            credentials are supplied using one of these mechanisms by the environment (e.g. deployed from configuration
            management, set as environment variables etc.)

            Refer: http://boto3.readthedocs.io/en/latest/guide/configuration.html

    """

    def __init__(self, bucket, prefix, config, logger, archive_mode=False):
        super(S3UploadRunner, self).__init__(config, logger, archive_mode)

        self.bucket = bucket
        self.prefix = prefix

        self.s3_client = boto3.client('s3')

    def _delete_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)
        self.s3_client.delete_object(Bucket=self.bucket, Key=abs_path)

    def _get_absolute_dest_uri(self, pipeline_file):
        return "s3://{bucket}/{path}".format(bucket=self.bucket, path=self._get_absolute_dest_path(pipeline_file))

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        self._validate_bucket()

    def _upload_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)

        with open(pipeline_file.src_path, 'rb') as f:
            self.s3_client.upload_fileobj(f, Bucket=self.bucket, Key=abs_path,
                                          ExtraArgs={'ContentType': pipeline_file.mime_type})

    def _validate_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            raise InvalidUploadUrlError(
                "unable to access S3 bucket '{0}': {1}".format(self.bucket, format_exception(e)))

    def determine_overwrites(self, pipeline_files):
        validate_pipelinefilecollection(pipeline_files)

        for pipeline_file in pipeline_files:
            abs_path = self._get_absolute_dest_path(pipeline_file)
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=abs_path)
            pipeline_file.is_overwrite = bool(k for k in response.get('Contents', []) if k['Key'] == abs_path)


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


class SftpUploadRunner(BaseUploadRunner):
    """UploadRunner to upload files to an SFTP server

    Note: similar to the S3 upload runner, this does not implement any authentication code, as this is better handled by
            the environment in the form of public key authentication

    """

    def __init__(self, server, prefix, config, logger, archive_mode=False):
        super(SftpUploadRunner, self).__init__(config, logger, archive_mode)
        self.server = server
        self.prefix = prefix

        self._sshclient = SSHClient()

        # TODO: replace with more sensible policy... predetermined keys?
        self._sshclient.set_missing_host_key_policy(AutoAddPolicy())

        self.sftp_client = None

    def _connect_sftp(self):
        self._sshclient.connect(self.server)
        self.sftp_client = self._sshclient.open_sftp()

    def _delete_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)
        self.sftp_client.remove(abs_path)

    def _get_absolute_dest_uri(self, pipeline_file):
        return "sftp://{server}{path}".format(server=self.server, path=self._get_absolute_dest_path(pipeline_file))

    def _post_run_hook(self):
        return

    def _pre_run_hook(self):
        self._connect_sftp()

    def _upload_file(self, pipeline_file):
        abs_path = self._get_absolute_dest_path(pipeline_file)
        parent_dir = os.path.dirname(abs_path)
        sftp_mkdir_p(self.sftp_client, parent_dir)

        with open(pipeline_file.src_path, 'rb') as f:
            self.sftp_client.putfo(f, abs_path, confirm=True)

    def determine_overwrites(self, pipeline_files):
        validate_pipelinefilecollection(pipeline_files)

        for pipeline_file in pipeline_files:
            abs_path = self._get_absolute_dest_path(pipeline_file)
            pipeline_file.is_overwrite = sftp_path_exists(self.sftp_client, abs_path)
