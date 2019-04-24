FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y git python-pip libxml2-dev libxslt-dev python-dev python-cffi

ENV HDF5_DIR=/usr/include/hdf5/
RUN apt-get install -y libhdf5-dev libnetcdf-dev

RUN pip install Cython wheel setuptools numpy