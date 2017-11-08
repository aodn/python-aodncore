import smtplib
import socket

from aodncore.pipeline import NotificationRecipientType
from aodncore.pipeline.steps.notify import (get_child_notify_runner, EmailNotifyRunner, LogFailuresNotifyRunner,
                                            NotifyList, NotificationRecipient, SnsNotifyRunner)
from aodncore.testlib import BaseTestCase, mock

TEST_NOTIFICATION_DATA = {
    'input_file': '',
    'processing_result': 'HANDLER_SUCCESS',
    'handler_start_time': '2017-10-23 16:05',
    'collection_headers': [],
    'collection_data': [],
    'notify_params': {},
    'error_details': ''
}


class TestPipelineStepsNotify(BaseTestCase):
    def test_get_check_runner(self):
        with self.assertRaises(ValueError):
            _ = get_child_notify_runner(1, None, None, self.mock_logger)
        with self.assertRaises(ValueError):
            _ = get_child_notify_runner('str', None, None, self.mock_logger)

        email_runner = get_child_notify_runner(NotificationRecipientType.EMAIL, None, None, self.mock_logger)
        self.assertIsInstance(email_runner, EmailNotifyRunner)

        sns_runner = get_child_notify_runner(NotificationRecipientType.SNS, None, None, self.mock_logger)
        self.assertIsInstance(sns_runner, SnsNotifyRunner)

        fail_runner = get_child_notify_runner(NotificationRecipientType.INVALID, None, None, self.mock_logger)
        self.assertIsInstance(fail_runner, LogFailuresNotifyRunner)


class TestEmailNotifyRunner(BaseTestCase):
    def setUp(self):
        super(TestEmailNotifyRunner, self).setUp()
        self.email_runner = EmailNotifyRunner(TEST_NOTIFICATION_DATA, self.config, self.mock_logger)
        self.notify_list = NotifyList()

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @mock.patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_email_success(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.sendmail.return_value = {}

        recipient = NotificationRecipient.from_string('email:nobody@example.com')
        self.notify_list.add(recipient)
        self.email_runner.run(self.notify_list)

        mock_smtp.return_value.sendmail.assert_called_once()
        self.assertTrue(recipient.notification_succeeded)
        self.assertIsNone(recipient.error)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @mock.patch('aodncore.pipeline.steps.notify.TemplateRenderer')
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

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @mock.patch('aodncore.pipeline.steps.notify.TemplateRenderer')
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

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @mock.patch('aodncore.pipeline.steps.notify.TemplateRenderer')
    def test_recipients_one_failed(self, mock_templaterenderer, mock_smtp):
        mock_templaterenderer.return_value.render.return_value = 'DUMMY EMAIL BODY'
        mock_smtp.return_value.sendmail.return_value = {'recipient1@example.com': (550, "User unknown")}

        recipient1 = NotificationRecipient.from_string('email:recipient1@example.com')
        recipient2 = NotificationRecipient.from_string('email:recipient2@example.com')
        self.notify_list.add(recipient1)
        self.notify_list.add(recipient2)
        self.email_runner.run(self.notify_list)

        mock_smtp.return_value.sendmail.assert_called_once()
        self.assertFalse(recipient1.notification_succeeded)
        self.assertIsNotNone(recipient1.error)
        self.assertTrue(recipient2.notification_succeeded)
        self.assertIsNone(recipient2.error)
        self.assertIsNone(self.email_runner.error)

    @mock.patch('aodncore.pipeline.steps.notify.smtplib.SMTP')
    @mock.patch('aodncore.pipeline.steps.notify.TemplateRenderer')
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

        mock_smtp.return_value.sendmail.assert_called_once()
        self.assertFalse(recipient1.notification_succeeded)
        self.assertFalse(recipient2.notification_succeeded)

        self.assertIsNotNone(recipient1.error)
        self.assertIsNotNone(recipient1.error)

        self.assertIsNone(self.email_runner.error)


class TestLogFailuresNotifyRunner(BaseTestCase):
    def setUp(self):
        super(TestLogFailuresNotifyRunner, self).setUp()
        self.fail_runner = LogFailuresNotifyRunner(TEST_NOTIFICATION_DATA, self.config, self.mock_logger)
        self.notify_list = NotifyList()

    def test_invalid_recipient(self):
        recipient1 = NotificationRecipient.from_string('invalid:recipient1')
        self.notify_list.add(recipient1)
        try:
            self.fail_runner.run(self.notify_list)
        except Exception as e:
            raise AssertionError(
                "unexpected exception raised. {cls} {msg}".format(cls=e.__class__.__name__, msg=e))
        self.mock_logger.warning.assert_called_with("recipients unable to be notified: ['invalid:recipient1']")
