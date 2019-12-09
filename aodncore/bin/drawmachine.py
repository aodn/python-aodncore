#!/usr/bin/env python

"""Simple script to generate a nice state machine diagram of the HandlerBase Machine

    .. note:: requires pygraphviz to be installed, which is omitted from requirements.txt due to not being required in
        prod. Also requires various system packages:

            Example for Ubuntu: $ sudo apt install python-dev graphviz libgraphviz-dev pkg-config
"""

import argparse
import tempfile
from datetime import datetime
from functools import partial
from unittest.mock import MagicMock, patch
from transitions.extensions import GraphMachine

from aodncore.pipeline.handlerbase import HandlerBase
from aodncore.pipeline.watch import IncomingFileStateManager


def draw_diagram(output_file, class_name):
    machine = partial(GraphMachine, show_conditions=False, show_auto_transitions=False,
                      title="{class_name} State Machine - {timestamp}".format(class_name=class_name,
                                                                              timestamp=datetime.now()))

    if class_name == 'HandlerBase':
        with patch('aodncore.pipeline.handlerbase.Machine', new=machine), \
             patch('aodncore.pipeline.handlerbase.validate_lazyconfigmanager', new=lambda p: True):
            m = HandlerBase(None)
    elif class_name == 'IncomingFileStateManager':
        with patch('aodncore.pipeline.watch.Machine', new=machine):
            m = IncomingFileStateManager('', None, None, MagicMock(), None, None, None, None)

    m.get_graph().draw(output_file, prog='dot')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--class-name', default='HandlerBase', choices=['HandlerBase', 'IncomingFileStateManager'])
    parser.add_argument('--output-file', default=tempfile.mktemp(prefix='aodn_pipeline-', suffix='.png'))
    args = parser.parse_args()
    print("Writing state machine diagram to: {output_file}".format(output_file=args.output_file))
    draw_diagram(args.output_file, args.class_name)
