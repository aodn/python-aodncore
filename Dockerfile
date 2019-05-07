FROM ubuntu:16.04

ENV HDF5_DIR=/usr/include/hdf5

RUN apt-get update && apt-get install -y --no-install-recommends \
    python-pip \
    python-dev \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    libhdf5-dev \
    libnetcdf-dev \
    libudunits2-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    bump2version==0.5.10 \
    Cython \
    wheel \
    setuptools \
    numpy
