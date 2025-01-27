#!/bin/sh
# convenient to ensure everything is recompiled
go run $(ls *.go | grep -v '_test\.go$') $@
