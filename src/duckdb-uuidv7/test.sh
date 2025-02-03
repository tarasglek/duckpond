#!/bin/sh
set -x
duckdb -init uuidv7.sql -csv < test.sql |grep true && echo Test passed
