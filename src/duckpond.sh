#!/bin/bash
# convenient to ensure everything is recompiled
pushd $(basename $0)
go run $(ls *.go | grep -v '_test\.go$') $@
popd