#!/usr/bin/env bash
set -e
DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${DIR}

DATA_DIR=$(realpath ../data/)
mkdir -p ${DATA_DIR}/{csv,xml}
mkdir -p ${DATA_DIR}/csv/{index,listing,xbrl}