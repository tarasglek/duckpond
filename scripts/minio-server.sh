#!/bin/bash

export MINIO_ROOT_USER=demo
export MINIO_ROOT_PASSWORD=demo-pass

minio server s3/data --console-address ":9001"
