# Minimal makefile for Sphinx documentation
#

SHELL := /bin/bash

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXAPIDOC  = sphinx-apidoc
SPHINXBUILD   = sphinx-build
SPHINXPROJ    = aodncore
SOURCEDIR     = .
BUILDDIR      = _build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: apidoc generate help nojekyll publish Makefile

clean:
	rm -rf aodncore*.rst modules.rst _build

apidoc:
	pushd ..; $(SPHINXAPIDOC) $(SPHINXPROJ) -o sphinx; popd

nojekyll:
	touch _build/html/.nojekyll

generate: clean apidoc html nojekyll

deploy:
	pushd ..; GIT_DEPLOY_DIR=sphinx/_build/html sphinx/deploy.sh; popd

publish: generate deploy

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)