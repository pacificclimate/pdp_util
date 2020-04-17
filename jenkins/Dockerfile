# -- DISCLAIMER --
# This Dockerfile is used by Jenkins to create a test environment.
# It does NOT create a containerized instance of crmprtd.
FROM ubuntu:18.04

# Set up locales
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y && \
	apt-get install -y \
		python-minimal \
		python-pip \
		postgresql-10-postgis-2.4 \
		postgresql-server-dev-10

# Create custom user
RUN useradd -ms /bin/bash -u 1000 tester

# Set up some directories for python installations
RUN mkdir -p /usr/local/lib/python${PYTHON_MAJOR}/ && \
	chown -R tester /usr/local/lib/python${PYTHON_MAJOR} && \
	chown -R tester /usr/local/bin/

# Use custom user
USER tester
