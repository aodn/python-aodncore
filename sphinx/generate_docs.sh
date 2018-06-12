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
}

generate_apidoc
generate_html
