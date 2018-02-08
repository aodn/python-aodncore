#!/usr/bin/env bash

set -eu

function generate_apidoc() {
    make clean
    pushd ..
    sphinx-apidoc aodncore -o sphinx
    popd
}

function generate_html() {
    make html
    touch _build/html/.nojekyll
    rsync --verbose --recursive --delete _build/html/ ../docs
}

generate_apidoc
generate_html
