FROM python:3.7-buster

RUN apt-get update && apt-get install -y telnet

COPY requirements.txt requirements.txt
RUN python -m venv venv
RUN venv/bin/pip install -r requirements.txt

COPY zerog zerog
COPY tests tests
