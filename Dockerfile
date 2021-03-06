FROM ubuntu:16.04

ARG BUILDER_UID=9999

ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV PATH /home/builder/.local/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    libmagic1 \
    libudunits2-dev \
    python3-dev \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10

RUN wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py pip==18.1 setuptools==49.6.0 wheel==0.35.1 \
    && rm -rf get-pip.py

RUN pip install \
    Cython==0.29 \
    bump2version==0.5.10 \
    sphinx==2.2.2 \
    sphinx_rtd_theme==0.4.3

RUN useradd --create-home --no-log-init --shell /bin/bash --uid $BUILDER_UID builder
USER builder
WORKDIR /home/builder
