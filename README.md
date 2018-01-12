## Python Pipeline

[![Build Status](https://travis-ci.org/aodn/python-aodncore.png?branch=master)](https://travis-ci.org/aodn/python-aodncore)

At the core of the handler base class is a state machine defining the workflow states and the transitions between those states. The following diagram describes the machine (generated directly from the codebase using https://github.com/aodn/python-aodncore/blob/master/aodncore/bin/drawmachine.py):

![Pipeline Handler state machine](https://github.com/aodn/python-aodncore/blob/master/state_machine.png)

The transition methods can be loosely divided into two categories:
1. Core methods - implement core common code such as logging (text and DB), setup of the central "file collection" used throughout the handler, compliance checking, publishing (harvesting/uploading). These are generally *not* intended to be overwritten by handler child classes.
1. Handler-specific methods - implement code specific to the given handler. These are interleaved between the core methods and allow data-specific manipulation of the workflow, such as controlling which files are checked/harvested/uploaded.

## States
### Linear states
1. initial
1. initialised
1. resolved
1. **preprocessed**
1. checked
1. **processed**
1. published
1. **postprocessed**
### Notify state
* notified_success
* notified_error
### Final states
* completed
* completed_with_errors

Each state transition executes a method before transitioning into that state, but the methods corresponding with the states in **bold** (preprocess/process/postprocess) are designed to be overridden in the handler class (i.e. the implementation in the base class does nothing). This allows the data specific handler code to perform any specific additional checking, generation of products, flagging/unflagging of files for checking/harvesting/uploading and any other control that the handler needs over the "file collection" being processed.

### Local setup and unit testing:
The unit tests can be executed in any number of ways, but the two simplest and most useful are:
1. Load the project in PyCharm IDE and use integrated testing (will still need to install the compliance checker dependencies manually, see below for how to do that)
2. Set up a dedicated virtual environment with the dependencies installed, and run tests via setup.py
```bash
# ASSUMED TO BE IN ROOT OF GIT CHECKOUT

~$ sudo apt install python-dev libhdf5-dev libnetcdf-dev libxml2-dev libxslt-dev libfreetype6-dev libudunits2-0
~$ virtualenv pltest
~$ source pltest/bin/activate
(pltest) ~$ pip install numpy
(pltest) ~$ easy_install https://jenkins.aodn.org.au/view/compliance_checker/job/compliance_checker_prod/lastSuccessfulBuild/artifact/dist/compliance_checker-2.3.1-py2.7.egg
(pltest) ~$ easy_install https://jenkins.aodn.org.au/view/cc_plugin_imos/job/cc_plugin_imos_prod/lastSuccessfulBuild/artifact/lib/cc_plugin_imos/dist/cc_plugin_imos-1.1.2-py2.7.egg
(pltest) ~$ pip install -r aodncore/requirements.txt
(pltest) ~$ pip install -e aodncore
(pltest) ~$ pip install -r aodndata/requirements.txt
(pltest) ~$ pip install mock   # only needed for tests

(pltest) ~$ cd aodncore
(pltest) ~$ python setup.py test

(pltest) ~$ cd ../aodndata
(pltest) ~$ python setup.py test
```

Both of these options should scan the project for tests, and execute them. The tests should be found in https://github.com/aodn/python-aodncore/tree/master/test_aodncore
