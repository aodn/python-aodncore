# AODN Core Pipeline

The `aodncore` package provides the core & generic functionality for all data ingestion pipelines at the AODN. This can be customised and extended for specific data-streams in the [`aodndata`](https://github.com/aodn/python-aodndata) package.

The package provides the base class for each pipeline handler, aodncore.pipeline.HandlerBase. This is the starting point for any new handler development, as it contains all of the core functionality of each handler, which is then available to the child class via class inheritance.

![python-aodncore](https://github.com/aodn/python-aodncore/workflows/python-aodncore/badge.svg)
[![coverage](https://codecov.io/gh/aodn/python-aodncore/branch/master/graph/badge.svg)](https://codecov.io/gh/aodn/python-aodncore)

Project documentation is hosted at: https://aodn.github.io/python-aodncore/index.html

## Testing
You can run pip install -e . from the project root to install the aodn core from local source during testing

## Licensing
This project is licensed under the terms of the GNU GPLv3 license.