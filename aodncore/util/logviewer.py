import os
import re
import sys
from collections import OrderedDict

LOGDIR_BASE = '/sw/chef/src/tmp/p2_logs'
LOG_WATCH = LOGDIR_BASE + '/watchservice/pipeline_watchservice-stderr.log'
LOGDIR_CELERY = LOGDIR_BASE + '/celery'
LOGDIR_PROCESS = LOGDIR_BASE + '/process'

# regular expressions to match log format and define fields extracted from log
LOG_FIELDS = OrderedDict([
    ('time', r"(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+"),
    ('level', r"(?P<level>[A-Z]+)\s+"),
    ('task_name', r"tasks.(?P<task_name>\w+)"),
    ('task_id', r"\[(?P<task_id>[0-9a-f-]+)\]\s+"),
    ('message', r"(?P<message>.*)")
])
INPUT_REGEX = re.compile(''.join(LOG_FIELDS.values()))
DEFAULT_FORMAT = '{time:20} {level:>9} {message}\n'


class LogViewer(object):
    """
    Class to parse logs written by pipelines and output various filtered or summary views.
    """

    def __init__(self, logfile):
        if not os.path.isfile(logfile):
            raise ValueError('{logfile}: no such file!'.format(logfile=logfile))
        self.logfile = logfile

    def log_entries(self):
        """Parse the log and return a tuple (raw, data) for one log entry at a time, where
        raw is te full text from the log, and data is a dictionary of extracted fields as
        per INPUT_REGEX.

        """
        # TODO: option to read from stdin
        with open(self.logfile) as log:
            for line in log:
                line = line.strip()
                m = INPUT_REGEX.match(line)
                if m is None:
                    # TODO: deal with unformatted lines
                    continue
                data = m.groupdict()

                yield line, data

    def filtered_entries(self, task_id=None, levels=None, pattern=None):
        """
        Filter the tuples returned by log_entries according to the filters specified.

        :param str task_id: only include log for given task uuid
        :param list levels: only include include messages with the given logging levels
        :param str pattern: only include log messages matching pattern (regular expression)
        :return: tuple (raw, data) as for log_entries

        """
        if pattern:
            pattern = re.compile(pattern)

        for raw, data in self.log_entries():
            if task_id and data['task_id'] != task_id:
                continue
            if levels and data['level'] not in levels:
                continue
            if pattern and not pattern.search(data['message']):
                continue
            # TODO: filter by handler step?
            yield raw, data

    def show(self, task_id=None, levels=None, pattern=None, fmt=DEFAULT_FORMAT):
        """
        Print a filtered & re-formatted view of the log to stdout

        :param str task_id: only include log for given task uuid
        :param list levels: only include include messages with the given logging levels
        :param str pattern: only include log messages matching pattern (regular expression)
        :param str fmt: output format (fmt.format() applied to dict of LOG_FIELDS extracted from log)

        """
        for raw, data in self.filtered_entries(task_id=task_id, levels=levels, pattern=pattern):
            line_out = fmt.format(**data)
            try:
                sys.stdout.write(line_out)
                sys.stdout.flush()
            except IOError:
                # this can happen if output is piped to `head` or `less`
                pass