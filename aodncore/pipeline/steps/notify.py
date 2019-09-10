"""This module provides the step runner classes for the :ref:`notify` step.

Notification is performed by a :py:class:`BaseNotifyRunner` class, which interacts with an endpoint representing a
notification protocol, in order to send a report detailing the status of the files processed by a handler class.

The most common use of this step is to send email notifications.
"""

import abc
import os
import smtplib
from collections import OrderedDict
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from tempfile import SpooledTemporaryFile
from zipfile import ZipFile

from tabulate import tabulate

from .basestep import BaseStepRunner
from ..common import (NotificationRecipientType, validate_recipienttype)
from ..exceptions import InvalidRecipientError, NotificationFailedError
from ...util import (IndexedSet, TemplateRenderer, format_exception, lazyproperty, validate_bool, validate_dict,
                     validate_nonstring_iterable, validate_type)
import six

__all__ = [
    'get_notify_runner',
    'NotifyRunnerAdapter',
    'EmailNotifyRunner',
    'LogFailuresNotifyRunner',
    'NotifyList',
    'NotificationRecipient',
    'SnsNotifyRunner'
]


def get_notify_runner(notification_data, config, logger, notify_params=None):
    """Factory function to return notify runner class

    :param notification_data: dictionary containing notification data (i.e. template values)
    :param config: :py:class:`LazyConfigManager` instance
    :param logger: :py:class:`Logger` instance
    :param notify_params: dict of parameters to pass to :py:class:`BaseNotifyRunner` class for runtime configuration
    :return: :py:class:`BaseNotifyRunner` class
    """
    return NotifyRunnerAdapter(notification_data, config, logger, notify_params)


def get_child_notify_runner(recipient_type, notification_data, config, logger):
    """Factory function to return appropriate notify runner based on recipient type value

    :param recipient_type: :py:class:`NotificationRecipientType` enum member
    :param notification_data: dict containing values used in templating etc.
    :param config: :py:class:`LazyConfigManager` instance
    :param logger: :py:class:`Logger` instance
    :return: :py:class:`BaseNotifyRunner` class
    """
    validate_recipienttype(recipient_type)

    if recipient_type is NotificationRecipientType.EMAIL:
        return EmailNotifyRunner(notification_data, config, logger)
    elif recipient_type is NotificationRecipientType.SNS:
        return SnsNotifyRunner(notification_data, config, logger)
    else:
        return LogFailuresNotifyRunner(notification_data, config, logger)


class BaseNotifyRunner(six.with_metaclass(abc.ABCMeta, BaseStepRunner)):
    """Base class for NotifyRunner classes, provides *protocol agnostic* helper methods and properties for child
        NotifyRunner classes
    """

    def __init__(self, notification_data, config, logger):
        super(BaseNotifyRunner, self).__init__(config, logger)
        self.notification_data = notification_data

        self.error = None
        self._message_parts = None
        self._template_values = None

    @abc.abstractmethod
    def run(self, notify_list):
        pass

    @lazyproperty
    def message_parts(self):
        """Returns a tuple containing the rendered text and HTML templates

        :return: tuple containing (text_part, html_part)
        """
        message_parts = self._render()
        return message_parts

    @lazyproperty
    def template_values(self):
        """Assemble the template values from the supplied notification data and rendered file tables

        :return: dict containing final template values
        """
        tables = self._get_file_tables()
        template_values = self.notification_data.copy()
        template_values.update(tables)
        return template_values

    @staticmethod
    def _get_html_input_file_table(table_data):
        html_lines = ["<table><tbody>"]
        row_template = '<tr><th style="text-align: left;">{k}</th><td style="text-align: left;">{v}</td></tr>'
        rows = [row_template.format(k=k, v=v) for k, v in table_data.items()]
        html_lines.extend(rows)
        html_lines.append("</tbody></table>")
        html = os.linesep.join(html_lines)
        return html

    @staticmethod
    def _get_text_input_file_table(table_data):
        text_lines = []
        row_template = '{k}: {v}'
        rows = [row_template.format(k=k, v=v) for k, v in table_data.items()]
        text_lines.extend(rows)
        text = os.linesep.join(text_lines)
        return text

    def _get_file_tables(self):
        """Render tables for use in notifications

            .. note:: everything in this method assumes *strict ordering* of elements, hence use of :py:class:`list` and
                :py:class:`OrderedDict` types, rather than potentially more efficient :py:class:`dict` and set types

        :return: :py:class:`dict` containing rendered input file and file collection tables, in text and HTML format
        """

        input_file_table_data = OrderedDict([
            ('Input file', self.notification_data['input_file']),
            ('Uploaded to', self.notification_data['upload_dir']),
            ('Processed at', self.notification_data['handler_start_time']),
            ('Compliance checks', self.notification_data['checks']),
            ('Result', self.notification_data['processing_result'])
        ])

        text_input_file_table = self._get_text_input_file_table(input_file_table_data)
        html_input_file_table = self._get_html_input_file_table(input_file_table_data)

        # column ordering and inclusion for collection table is determined entirely from this collection
        included_columns = ('name', 'check_passed', 'published')

        attribute_friendly_name_map = {
            'name': 'Name',
            'check_passed': 'Checks passed',
            'published': 'Published?'
        }

        # this validates that only existing columns are included, and becomes the authoritative list of included
        # columns, used when generating final headers and data rows
        raw_headers = [h for h in included_columns if h in self.notification_data['collection_headers']]

        # determine final column names by checking the "friendly" map for overrides
        collection_headers = [attribute_friendly_name_map.get(h, h) for h in raw_headers]

        # generate a "list of lists", where each element is the row containing only the desired elements (ordered)
        collection_data = [[pf[attr] for attr in raw_headers] for pf in self.notification_data['collection_data']]

        text_collection_table = tabulate(collection_data, collection_headers, tablefmt='simple')
        html_collection_table = tabulate(collection_data, collection_headers, tablefmt='html')

        return {
            'text_input_file_table': text_input_file_table,
            'html_input_file_table': html_input_file_table,
            'text_collection_table': text_collection_table,
            'html_collection_table': html_collection_table
        }

    @staticmethod
    def _get_recipient_addresses(notify_list):
        """Get a list of *only* the address attributes of the :py:class:`NotifyList`

        :param notify_list: :py:class:`NotifyList` instance from which to retrieve addresses
        :return: :py:class:`list` of addresses
        """
        recipient_addresses = [r.address for r in notify_list]
        return recipient_addresses

    def _render(self):
        template_renderer = TemplateRenderer()
        text = template_renderer.render(self._config.pipeline_config['templating']['text_notification_template'],
                                        self.template_values)
        html = template_renderer.render(self._config.pipeline_config['templating']['html_notification_template'],
                                        self.template_values)
        return text, html


class NotifyRunnerAdapter(BaseNotifyRunner):
    def __init__(self, notification_data, config, logger, notify_params):
        super(NotifyRunnerAdapter, self).__init__(notification_data, config, logger)
        self.notification_data = notification_data

        if notify_params is None:
            notify_params = {}

        self.notify_params = notify_params

    def run(self, notify_list):
        notify_list_object = NotifyList.from_collection(notify_list)
        invalid_recipients = notify_list_object.filter_by_notify_type(NotificationRecipientType.INVALID)
        if invalid_recipients:
            self._logger.error(
                "notifications unable to be sent to invalid recipients: {invalid}".format(
                    invalid=list((r.raw_string, r.error) for r in invalid_recipients)))
            invalid_recipients.set_notification_attempted()

        notify_types = {t.notify_type for t in notify_list_object if
                        t.notify_type is not NotificationRecipientType.INVALID}

        for notify_type in notify_types:
            type_notify_list = notify_list_object.filter_by_notify_type(notify_type)
            notify_runner = get_child_notify_runner(notify_type, self.notification_data, self._config, self._logger)
            self._logger.sysinfo("get_child_notify_runner -> {notify_runner}".format(notify_runner=notify_runner))
            notify_runner.run(type_notify_list)

        failed_notifications = notify_list_object.filter_by_failed()
        if failed_notifications:
            self._logger.error(
                "notifications failed to the following recipients: {failed}".format(
                    failed=list((r.raw_string, r.error) for r in failed_notifications)))
            succeeded_notifications = notify_list_object.filter_by_succeeded()
            if succeeded_notifications:
                self._logger.info("notifications succeeded to the following recipients: {succeeded}".format(
                    succeeded=list(r.raw_string for r in succeeded_notifications)))
        else:
            self._logger.info('all notification attempts were successful')

        return notify_list_object


class EmailNotifyRunner(BaseNotifyRunner):
    def _construct_message(self, recipient_addresses, subject, from_address):
        rendered_text, rendered_html = self.message_parts

        text_part = MIMEText(rendered_text, 'text')
        html_part = MIMEText(rendered_html, 'html')

        message = MIMEMultipart('mixed')
        message['Subject'] = subject
        message['From'] = from_address
        message['To'] = ','.join(recipient_addresses)

        message_related = MIMEMultipart('related')
        message_related.attach(html_part)

        message_alternative = MIMEMultipart('alternative')
        message_alternative.attach(text_part)
        message_alternative.attach(message_related)

        message.attach(message_alternative)

        failed_files = [f for f in self.notification_data['collection_data'] if
                        f['check_log'] and f['check_passed'] != 'True']
        if failed_files:
            attachment = MIMEBase('application', 'zip')

            with SpooledTemporaryFile(prefix='error_logs', suffix='.zip') as attachment_file:
                with ZipFile(attachment_file, 'w') as z:
                    for failed_file in failed_files:
                        path = "{failed_file[name]}.log.txt".format(failed_file=failed_file)
                        content = failed_file['check_log'].encode('utf-8')
                        z.writestr(path, content)

                attachment_file.seek(0)
                attachment.set_payload(attachment_file.read())

            encoders.encode_base64(attachment)
            attachment.add_header('Content-Disposition', 'attachment', filename='error_logs.zip')
            message.attach(attachment)

        return message

    def _send(self, recipient_addresses, message):
        smtp_server = smtplib.SMTP(timeout=60)
        sendmail_result = None
        try:
            smtp_server.connect(self._config.pipeline_config['mail']['smtp_server'],
                                port=self._config.pipeline_config['mail'].get('smtp_port', 587))
            if self._config.pipeline_config['mail'].get('smtp_tls', True):
                smtp_server.starttls()
            smtp_server.login(self._config.pipeline_config['mail']['smtp_user'],
                              self._config.pipeline_config['mail']['smtp_pass'])
            sendmail_result = smtp_server.sendmail(self._config.pipeline_config['mail']['from'], recipient_addresses,
                                                   message.as_string())
        finally:
            try:
                smtp_server.quit()
            except smtplib.SMTPServerDisconnected:
                pass
            except Exception as e:
                self._logger.warning("exception thrown when closing SMTP. {e}".format(e=format_exception(e)))

        return sendmail_result

    def run(self, notify_list):
        """Attempt to send notification email to recipients in notify_list parameter.

        The status of each individual attempt is stored in a :py:class:`dict` instance, as described in the
        :py:meth:`smtplib.SMTP.sendmail` method docs, which allows per-recipient status inspection/error logging.

        :param notify_list: :py:class:`NotifyList` instance
        :return: None
        """
        validate_notifylist(notify_list)

        recipient_addresses = self._get_recipient_addresses(notify_list)
        self._logger.info("email recipients: {recipient_addresses}".format(recipient_addresses=recipient_addresses))

        subject = self._config.pipeline_config['mail']['subject'].format(**self.template_values)
        from_address = self._config.pipeline_config['mail']['from']
        message = self._construct_message(recipient_addresses, subject, from_address)

        error_dict = None
        try:
            error_dict = self._send(recipient_addresses, message)
        except smtplib.SMTPRecipientsRefused as e:
            # the SMTP transaction was successful, but *all* of the recipients were refused by the destination server
            error_dict = e.recipients
        except Exception as e:
            # the SMTP transaction was unsuccessful, so consider the whole execution as having failed
            self._logger.exception(e)
            self.error = e
            notify_list.set_error(e)
        finally:
            if error_dict is not None:
                # use the error_dict to update each recipient status individually
                notify_list.update_from_error_dict(error_dict)
            notify_list.set_notification_attempted()


class SnsNotifyRunner(BaseNotifyRunner):
    def run(self, notify_list):
        validate_notifylist(notify_list)

        # TODO: implement SNS runner
        fail_runner = LogFailuresNotifyRunner(self.notification_data, self._config, self._logger)
        fail_runner.run(notify_list)


class LogFailuresNotifyRunner(BaseNotifyRunner):
    def run(self, notify_list):
        validate_notifylist(notify_list)

        recipients = list(r.raw_string for r in notify_list)
        self._logger.warning("recipients unable to be notified: {recipients}".format(recipients=recipients))


class NotifyList(object):
    __slots__ = ['__s']

    def __init__(self, data=None):
        super(NotifyList, self).__init__()

        self.__s = IndexedSet()

        if data is not None:
            self.update(data)

    def __contains__(self, element):
        return element in self.__s

    def __getitem__(self, index):
        result = self.__s[index]
        return NotifyList(result) if isinstance(result, IndexedSet) else result

    def __iter__(self):
        return iter(self.__s)

    def __len__(self):
        return len(self.__s)

    def __repr__(self):  # pragma: no cover
        return "{name}({repr})".format(name=self.__class__.__name__, repr=repr(list(self.__s)))

    def add(self, recipient):
        validate_notificationrecipient(recipient)

        result = recipient not in self.__s
        self.__s.add(recipient)
        return result

    # alias append to the add method
    append = add

    def discard(self, recipient):
        result = recipient in self.__s
        self.__s.discard(recipient)
        return result

    def difference(self, sequence):
        return self.__s.difference(sequence)

    def issubset(self, sequence):
        return self.__s.issubset(sequence)

    def issuperset(self, sequence):
        return self.__s.issuperset(sequence)

    def union(self, sequence):
        if not all(isinstance(f, NotificationRecipient) for f in sequence):
            raise TypeError('invalid sequence, all elements must be NotificationRecipient objects')
        return NotifyList(self.__s.union(sequence))

    def update(self, sequence):
        validate_nonstring_iterable(sequence)

        result = None
        for item in sequence:
            result = self.add(item)
        return result

    def filter_by_failed(self):
        return NotifyList(r for r in self.__s if r.notification_attempted and not r.notification_succeeded)

    def filter_by_succeeded(self):
        return NotifyList(r for r in self.__s if r.notification_attempted and r.notification_succeeded)

    def filter_by_notify_type(self, notify_type):
        """Return a new :py:class:`NotifyList` containing only recipients of the given notify_type

        :param notify_type: :py:class:`NotificationRecipientType` enum member by which to filter
            :py:class:`PipelineFile` instances
        :return: :py:class:`NotifyList` containing only :py:class:`NotifyRecipient` instances of the given type
        """
        validate_recipienttype(notify_type)
        collection = NotifyList(r for r in self.__s if r.notify_type is notify_type)
        return collection

    @classmethod
    def from_collection(cls, recipient_collection):
        return cls(NotificationRecipient.from_string(r) for r in recipient_collection)

    def set_notification_attempted(self):
        for recipient in self.__s:
            recipient.notification_attempted = True

    def set_error(self, error):
        """Set the error attribute for all elements

        :param error: :py:class:`Exception` instance
        :return: None
        """
        for recipient in self.__s:
            recipient.error = error

    def update_from_error_dict(self, error_dict):
        """Update recipient statuses according to the given error dictionary parameter. The absence of an address in the
        dict keys will be interpreted as "successfully sent".

        :param error_dict: dict as returned by :py:meth:`smtplib.SMTP.sendmail` method
        :return: None
        """
        validate_dict(error_dict)

        for recipient in self.__s:
            error_log = error_dict.get(recipient.address)
            if error_log is not None:
                recipient.error = NotificationFailedError("{0}: {1}".format(*error_log))
            else:
                recipient.notification_succeeded = True


class NotificationRecipient(object):
    def __init__(self, address, notify_type, raw_string='', error=None):
        self._address = address
        self._notify_type = None
        self.notify_type = notify_type
        self._raw_string = raw_string
        self.error = error

        self._notification_attempted = False
        self._notification_succeeded = False

    def __repr__(self):  # pragma: no cover
        return "{name}({str})".format(name=self.__class__.__name__, str=str(self.__dict__))

    @property
    def address(self):
        return self._address

    @property
    def notify_type(self):
        return self._notify_type

    @notify_type.setter
    def notify_type(self, notify_type):
        validate_recipienttype(notify_type)
        self._notify_type = notify_type

    @property
    def raw_string(self):
        return self._raw_string

    @property
    def notification_attempted(self):
        return self._notification_attempted

    @notification_attempted.setter
    def notification_attempted(self, notification_attempted):
        validate_bool(notification_attempted)
        self._notification_attempted = notification_attempted

    @property
    def notification_succeeded(self):
        return self._notification_succeeded

    @notification_succeeded.setter
    def notification_succeeded(self, notification_succeeded):
        validate_bool(notification_succeeded)
        self._notification_succeeded = notification_succeeded

    @classmethod
    def from_string(cls, recipient_string):
        """From a given 'recipient string', expected to be in the format of 'protocol:address', return a new
        :py:class:`NotificationRecipient` object with attributes set according to the content/validity of the input
        string

        :param recipient_string: string in format of 'protocol:address'
        :return: :py:class:`NotificationRecipient` object
        """
        try:
            protocol, address = recipient_string.split(':', 1)
        except ValueError:
            address = ''
            error = InvalidRecipientError('invalid recipient string')
            recipient_type = NotificationRecipientType.INVALID
        else:
            error = None
            recipient_type = NotificationRecipientType.get_type_from_protocol(protocol)

            address_is_valid = recipient_type.address_validation_function(address)
            if not address_is_valid:
                error = InvalidRecipientError(recipient_type.error_string)
                recipient_type = NotificationRecipientType.INVALID

        return cls(address, recipient_type, recipient_string, error)


validate_notifylist = validate_type(NotifyList)
validate_notificationrecipient = validate_type(NotificationRecipient)
