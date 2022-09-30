# AODN Core Pipeline for Prefect Flows

This branch modifies the original python-aodncore and allows the pipeline steps to be used independently of the original state machine. As a result the steps can be reordered, skipped, added to or otherwise combined into adhoc Prefect flows. The changes also make the original state machine loggers available to Prefect so that fine grained output can be viewed in the Prefect Cloud interface. This results in a high degree of flexibility in the implementation of pipeline flows.

The branch also includes functions for processing files in s3 buckets (aodncore/util/s3_util.py) and some minor changes to use these instead of the local file system.

A new class, aodncore/pipeline/prefecthandlerbase.py. PrefectHandlerBase extends HandlerBase and should be used instead of HandlerBase when creating pipelines for use in Prefect flows.

In PrefectHandlerBase the following HandlerBase methods are overridden:

`_set_input_file_attributes` is overridden to use the s3 ETag for the checksum

`_init_logger` is overridden to use the Prefect logger to log messages from aodncore to the Prefect Cloud UI.
 
`_resolve` is overridden so that resolve runners can move files from the landing s3 to the local filesystem instead of copying. This is currently only available for the SingleFileResolveRunner.

It is not intended for the Prefect pipelines to do the harvesting step. This would be part of additional Prefect flows downstream of the data ingestion. We have avoided the harvesting step by simply using a newly created NoHarvestRunner class in aodncore/pipeline/steps/harvest.py which will do the file upload without actually harvesting.

For additional details and notes on future possible work see https://github.com/aodn/backlog/issues/4290.

# Original AODN Core Pipeline. 

*Note: This branch overrides the behaviours described below. This section is only here for reference to the original AODN core pipeline.*

The `aodncore` package provides the core & generic functionality for all data ingestion pipelines at the AODN. This can be customised and extended for specific data-streams in the [`aodndata`](https://github.com/aodn/python-aodndata) package.

The package provides the base class for each pipeline handler, aodncore.pipeline.HandlerBase. This is the starting point for any new handler development, as it contains all of the core functionality of each handler, which is then available to the child class via class inheritance.

![python-aodncore](https://github.com/aodn/python-aodncore/workflows/python-aodncore/badge.svg)
[![coverage](https://codecov.io/gh/aodn/python-aodncore/branch/master/graph/badge.svg)](https://codecov.io/gh/aodn/python-aodncore)

Project documentation is hosted at: https://aodn.github.io/python-aodncore/index.html
## Licensing
This project is licensed under the terms of the GNU GPLv3 license.
