#!/usr/bin/env python

"""Simple script to generate a nice state machine diagram of the HandlerBase Machine

    .. note:: requires pygraphviz to be installed, which is omitted from requirements.txt due to not being required in
        prod.
"""

import argparse
import tempfile
from datetime import datetime
from functools import partial

from transitions.extensions import GraphMachine

from aodncore.pipeline import HandlerBase

try:
    from unittest import mock
except ImportError:
    import mock


def draw_diagram(output_file):
    machine = partial(GraphMachine, show_conditions=False, show_auto_transitions=False,
                      title="AODN Pipeline Handler - {timestamp}".format(timestamp=datetime.now()))

    with mock.patch('aodncore.pipeline.handlerbase.Machine', new=machine), \
         mock.patch('aodncore.pipeline.handlerbase.validate_lazyconfigmanager', new=lambda p: True):
        m = HandlerBase(None)
    m.get_graph().draw(output_file, prog='dot')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output-file', default=tempfile.mktemp(prefix='aodn_pipeline-', suffix='.png'))
    args = parser.parse_args()
    print("Writing state machine diagram to: {output_file}".format(output_file=args.output_file))
    draw_diagram(args.output_file)
