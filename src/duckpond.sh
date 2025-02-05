#!/bin/bash
# convenient to ensure everything is recompiled
pushd "$(dirname "$0")" > /dev/null
go run $(ls *.go | grep -v '_test\.go$') $@
popd > /dev/null
