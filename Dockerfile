FROM python:3.7-buster

# install libcouchbase
# Install Couchbase's GPG key
# Adding Ubuntu 18.04 repo to apt/sources.list of 18.10 or 19.04
RUN wget -O - http://packages.couchbase.com/ubuntu/couchbase.key | apt-key add - \
  && echo "deb http://packages.couchbase.com/ubuntu bionic bionic/main" | tee /etc/apt/sources.list.d/couchbase.list

# To install or upgrade packages
RUN apt-get update && apt-get install -y \
  build-essential \
  libcouchbase2-bin \
  libcouchbase-dev \
  telnet

COPY requirements.txt requirements.txt
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt

COPY zerog zerog
COPY tests tests
