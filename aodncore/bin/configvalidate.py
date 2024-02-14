#!/usr/bin/env python

"""Script to test the validity of pipeline configuration,

"""
import argparse
import os
from jsonschema.exceptions import ValidationError
from aodncore.pipeline.configlib import load_pipeline_config, validate_pipeline_config


def validate_config_file(pipeline_conf_file):
    try:
        print("validating pipeline config file '{path}'...".format(path=pipeline_conf_file))
        pipeline_config = load_pipeline_config(pipeline_conf_file)
        validate_pipeline_config(pipeline_config)
    except ValidationError as e:
        print("VALIDATION FAILED{nl}{nl}{e}".format(e=str(e), nl=os.linesep))
        return 1
    else:
        print('VALIDATION SUCCEEDED')
        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('path')
    args = parser.parse_args()

    exit(validate_config_file(args.path))
