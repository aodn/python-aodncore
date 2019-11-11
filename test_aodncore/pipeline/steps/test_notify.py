import os
import smtplib
import socket
from unittest.mock import MagicMock, patch
from aodncore.pipeline import NotificationRecipientType, PipelineFile, PipelineFileCollection
from aodncore.pipeline.steps.notify import (get_child_notify_runner, BaseNotifyRunner, EmailNotifyRunner,
                                            LogFailuresNotifyRunner, NotifyList, NotificationRecipient, SnsNotifyRunner)
from aodncore.testlib import BaseTestCase

TESTDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testdata')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')


def get_notification_data():
    collection = PipelineFileCollection(PipelineFile(GOOD_NC))
    collection_headers, collection_data = collection.get_table_data()

    data = {
        'input_file': 'good.nc',
        'processing_result': 'HANDLER_SUCCESS',
        'handler_start_time': '2017-10-23 16:05',
        'checks': None,
        'collection_headers': collection_headers,
        'collection_data': collection_data,
        'error_details': '',
        'upload_dir': None
    }

    return data


class TestPipelineStepsNotify(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(ValueError):
            _ = get_child_notify_runner(1, None, None, self.test_logger)
        with self.assertRaises(ValueError):
            _ = get_child_notify_runner('str', None, None, self.test_logger)

        email_runner = get_child_notify_runner(NotificationRecipientType.EMAIL, None, None, self.test_logger)
        self.assertIsInstance(email_runner, EmailNotifyRunner)

        sns_runner = get_child_notify_runner(NotificationRecipientType.SNS, None, None, self.test_logger)
        self.assertIsInstance(sns_runner, SnsNotifyRunner)

        fail_runner = get_child_notify_runner(NotificationRecipientType.INVALID, None, None, self.test_logger)
        self.assertIsInstance(fail_runner, LogFailuresNotifyRunner)


class DummyNotifyRunner(BaseNotifyRunner):
    def run(self, notify_list):
        pass


class TestBaseNotifyRunner(BaseTestCase):
    def setUp(self):
        super(TestBaseNotifyRunner, self).setUp()
        notification_data = get_notification_data()
        self.dummy_runner = DummyNotifyRunner(notification_data, self.config, self.test_logger)

    def test__get_file_tables(self):
        file_tables = self.dummy_runner._get_file_tables()

        expected_keys = ['html_collection_table', 'html_input_file_table', 'text_collection_table',
                         'text_input_file_table']

        self.assertCountEqual(expected_keys, list(file_tables.keys()))


class TestEmailNotifyRunner(BaseTestCase):
    def setUp(self):
        super(TestEmailNotifyRunner, self).setUp()
        notification_data = get_notification_data()
        self.email_runner = EmailNotifyRunner(notification_data, self.config, self.test_logger)
        self.notify_list = NotifyList()

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_email_success(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.sendmail.return_value = {}

        recipient = NotificationRecipient.from_string('email:nobody@example.com')
        self.notify_list.add(recipient)
        self.email_runner.run(self.notify_list)

        self.assertEqual(1, mock_smtp.return_value.sendmail.call_count)
        self.assertTrue(recipient.notification_succeeded)
        self.assertIsNone(recipient.error)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_invalid_login(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.login.side_effect = smtplib.SMTPException

        recipient = NotificationRecipient.from_string('email:invalid_email')
        self.notify_list.add(recipient)
        self.email_runner.run(self.notify_list)

        mock_smtp.return_value.sendmail.assert_not_called()
        self.assertFalse(recipient.notification_succeeded)
        self.assertIsNotNone(recipient.error)
        self.assertIsInstance(self.email_runner.error, smtplib.SMTPException)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_invalid_server(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.connect.side_effect = socket.gaierror

        recipient = NotificationRecipient.from_string('email:invalid_email')
        self.notify_list.add(recipient)
        self.email_runner.run(self.notify_list)

        mock_smtp.return_value.sendmail.assert_not_called()
        self.assertFalse(recipient.notification_succeeded)
        self.assertIsNotNone(recipient.error)
        self.assertIsInstance(self.email_runner.error, socket.gaierror)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_recipients_one_failed(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.sendmail.return_value = {'recipient1@example.com': (550, "User unknown")}

        recipient1 = NotificationRecipient.from_string('email:recipient1@example.com')
        recipient2 = NotificationRecipient.from_string('email:recipient2@example.com')
        self.notify_list.add(recipient1)
        self.notify_list.add(recipient2)
        self.email_runner.run(self.notify_list)

        self.assertEqual(1, mock_smtp.return_value.sendmail.call_count)
        self.assertFalse(recipient1.notification_succeeded)
        self.assertIsNotNone(recipient1.error)
        self.assertTrue(recipient2.notification_succeeded)
        self.assertIsNone(recipient2.error)
        self.assertIsNone(self.email_runner.error)

    @patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_recipients_all_failed(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.sendmail.side_effect = smtplib.SMTPRecipientsRefused(
            {'recipient1@example.com': (550, "User unknown"),
             'recipient2@example.com': (550, "User unknown")})

        recipient1 = NotificationRecipient.from_string('email:recipient1@example.com')
        recipient2 = NotificationRecipient.from_string('email:recipient2@example.com')
        self.notify_list.add(recipient1)
        self.notify_list.add(recipient2)
        self.email_runner.run(self.notify_list)

        self.assertEqual(1, mock_smtp.return_value.sendmail.call_count)
        self.assertFalse(recipient1.notification_succeeded)
        self.assertFalse(recipient2.notification_succeeded)

        self.assertIsNotNone(recipient1.error)
        self.assertIsNotNone(recipient1.error)

        self.assertIsNone(self.email_runner.error)


class TestLogFailuresNotifyRunner(BaseTestCase):
    def setUp(self):
        super(TestLogFailuresNotifyRunner, self).setUp()
        notification_data = get_notification_data()
        self.fail_runner = LogFailuresNotifyRunner(notification_data, self.config, MagicMock())
        self.notify_list = NotifyList()

    def test_invalid_recipient(self):
        recipient1 = NotificationRecipient.from_string('invalid:recipient1')
        self.notify_list.add(recipient1)

        with self.assertNoException():
            self.fail_runner.run(self.notify_list)

        self.fail_runner._logger.warning.assert_called_with("recipients unable to be notified: ['invalid:recipient1']")
