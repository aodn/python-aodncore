#!/usr/bin/env bash

set -eu

pushd $(git rev-parse --show-toplevel)
git subtree push --prefix sphinx/_build/html/ origin gh-pages
popd
