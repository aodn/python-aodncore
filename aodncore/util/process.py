"""This module provides general purpose code for interacting with operating system subprocesses
"""

from __future__ import absolute_import
import os
import subprocess

import six
from .misc import format_exception
from ..common import SystemCommandFailedError

__all__ = [
    'SystemProcess'
]


class SystemProcess(object):
    """Class to encapsulate a system command, including execution, output handling and returncode handling
    """

    def __init__(self, command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stdin_text=None, env=None, shell=False):
        super(SystemProcess, self).__init__()

        self.command = command
        self.shell = shell

        self.stdin = stdin
        self.stdout = stdout

        self.stdin_text = stdin_text
        self.stdout_text = ''
        self.stderr_text = ''

        _env = os.environ.copy()
        if env:
            _env = env
        self.env = _env

        self._executed = False

        self.validate_command()

    def validate_command(self):

        if self.shell is False:
            if not isinstance(self.command, list):
                raise SystemCommandFailedError("command parameter must be a list if not a bash shell command")
        else:
            if not isinstance(self.command, six.string_types):
                raise SystemCommandFailedError("command param must be a string if bash shell command")
            else:
                if not self.command:
                    raise SystemCommandFailedError("command param must not be empty if bash shell command")

    def execute(self):
        """Execute the system command

        :return: None
        """

        if self._executed:
            raise SystemCommandFailedError("command has already been executed")

        self._executed = True

        try:
            proc = subprocess.Popen(self.command, stdin=self.stdin, stdout=self.stdout, stderr=subprocess.STDOUT,
                                    shell=self.shell, env=self.env)
        except OSError as e:
            raise SystemCommandFailedError(
                "system command failed to execute. {e}".format(e=format_exception(e)))

        self.stdout_text, _ = proc.communicate(input=self.stdin_text)
        returncode = proc.wait()

        if returncode != 0:
            raise SystemCommandFailedError("system command exited with an error, output is {stderr}".format(
                stderr=self.stdout_text))
