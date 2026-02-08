#!/bin/sh
set -eu

awslocal s3 mb s3://demo-parquet-bucket || true
