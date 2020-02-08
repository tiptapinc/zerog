#!/bin/sh

pytest tests --cov=zerog --cov-report term-missing --cov-config=.coveragerc
