FROM ubuntu:18.04

# Get set to install packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update

# Set the locale
RUN apt-get -y install locales && \
    locale-gen en_CA.utf8
ENV LANG en_CA.utf8
ENV LANGUAGE en_CA:en
ENV LC_ALL en_CA.UTF-8

# Make legacy Ubuntu 18.04 postgres-9.5 packages available
RUN apt-get install -y curl ca-certificates gnupg && \
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    echo 'deb http://apt.postgresql.org/pub/repos/apt bionic-pgdg main' >/etc/apt/sources.list.d/pgdg.list && \
    apt-get update

# Install the packages required to install PyCDS.
RUN apt-get install -yq \
            libpq-dev \
            python2.7 \
            python2.7-dev \
            python-pip \
            postgresql-plpython-9.5 \
            postgresql-9.5-postgis-2.4 \
    && echo "done"
RUN pip install -U pip pytest

# You must mount local codebase to this directory
WORKDIR /codebase

# Install the local codebase when the container starts.
ENTRYPOINT ./docker/local-test/entrypoint.sh
