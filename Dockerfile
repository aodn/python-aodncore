FROM ubuntu:16.04

ARG BUILDER_UID=9999

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    libudunits2-dev \
    python-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py pip==18.1 \
    && rm -rf get-pip.py

RUN pip install \
    Cython==0.29

RUN useradd --create-home --no-log-init --shell /bin/bash --uid $BUILDER_UID builder
USER builder
WORKDIR /home/builder
