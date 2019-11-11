import os
import sys
import uuid

from aodncore.common import SystemCommandFailedError
from aodncore.testlib import BaseTestCase
from aodncore.util import SystemProcess

CAT_CMD = '/bin/cat'
ECHO_CMD = '/bin/echo'
PRINTENV_CMD = '/usr/bin/printenv'
if sys.platform == 'darwin':  # pragma: no cover
    FALSE_CMD = '/usr/bin/false'
    TRUE_CMD = '/usr/bin/true'
else:
    FALSE_CMD = '/bin/false'
    TRUE_CMD = '/bin/true'


class TestUtilProcess(BaseTestCase):
    def test_execute(self):
        echo_text = str(uuid.uuid4())
        process = SystemProcess([ECHO_CMD, echo_text])
        process.execute()
        self.assertEqual(echo_text, process.stdout_text.decode('utf-8').rstrip())

    def test_shell_execute(self):
        echo_text = str(uuid.uuid4())
        process = SystemProcess(''.join([ECHO_CMD, ' ', echo_text]), shell=True)
        process.execute()
        self.assertEqual(echo_text, process.stdout_text.decode('utf-8').rstrip())

    def test_execute_with_stdin(self):
        stdin_text = str(uuid.uuid4())
        process = SystemProcess([CAT_CMD])
        process.stdin_text = stdin_text.encode('utf-8')
        process.execute()
        self.assertEqual(stdin_text, process.stdout_text.decode('utf-8'))

    def test_execute_with_env(self):
        envname = str(uuid.uuid4())
        envvalue = str(uuid.uuid4())
        env = {envname: envvalue}
        process = SystemProcess([PRINTENV_CMD, envname], env=env)
        process.execute()
        self.assertEqual(envvalue, process.stdout_text.decode('utf-8').rstrip())

    def test_empty_command(self):
        with self.assertRaisesRegex(SystemCommandFailedError,
                                   'command parameter must be a list if not a bash shell command'):
            _ = SystemProcess(())

    def test_empty_shell_command(self):
        with self.assertRaisesRegex(SystemCommandFailedError,
                                   'command param must not be empty if bash shell command'):
            _ = SystemProcess('', shell=True)

    def test_empty_non_string_shell_command(self):
        with self.assertRaisesRegex(SystemCommandFailedError,
                                   'command param must be a string if bash shell command'):
            _ = SystemProcess((), shell=True)

    def test_nonexistent_command(self):
        command = [os.path.join('/nonexistent/path/with/a/{uuid}/in/the/middle'.format(uuid=uuid.uuid4()))]
        process = SystemProcess(command)
        with self.assertRaisesRegex(SystemCommandFailedError, ".*\[Errno 2\] No such file or directory"):
            process.execute()

    def test_execute_failure(self):
        process = SystemProcess([FALSE_CMD])
        with self.assertRaises(SystemCommandFailedError):
            process.execute()

    def test_execute_multiple(self):
        process = SystemProcess([TRUE_CMD])
        process.execute()
        with self.assertRaisesRegex(SystemCommandFailedError, '.*command has already been executed'):
            process.execute()
