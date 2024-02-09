#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# Process SEC/EDGAR XBRL filings.
# 1. Download master index files for XBRL filings.
# 2. Download XBRL XML files.
# 3. Parse XBRL XML files to generate BS/PL statements.
#
# Execution:
# ./run.sh 2022   # from year 2022 up to now
# ./run.sh 2022 2024  # from year 2022 to 2024
#--------------------------------------------------------------------------------
set -e
DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${DIR}

#--------------------------------------------------------------------------------
# Command line argument
# $1: YEAR
#--------------------------------------------------------------------------------
YEAR_END=$(date "+%Y")
if [ $# -lt 1 ] ; then
    echo "Usage: $(basename -- "$0") <year start> [<year end>]"
    echo "If <year end> is not provided, set to ${YEAR_END}"
    exit -1
fi
YEAR_START=$1
if [ $# -ge 2 ] ; then
    YEAR_END=$2
fi

#--------------------------------------------------------------------------------
# Create directories to save files.
#--------------------------------------------------------------------------------
DATA_DIR=$(realpath ../data/)
mkdir -p ${DATA_DIR}/{csv,xml}
mkdir -p ${DATA_DIR}/csv/{index,listing,xbrl}

#--------------------------------------------------------------------------------
# Process master index files and generate outputs.
#--------------------------------------------------------------------------------
chmod u+x sec_edgar_download_xbrl_indices.sh
EDGAR_MASTER_INDEX_BASE_URL="https://www.sec.gov/Archives/edgar/full-index"
YEAR=${YEAR_START}
while [ ${YEAR} -le ${YEAR_END} ]
do
    echo ""
    echo "--------------------------------------------------------------------------------"
    echo "Handling the XBRL index files for the year ${YEAR}..."
    # Download master index files for XBRL filings.
    ./sec_edgar_download_xbrl_indices.sh ${YEAR}
    python3 sec_edgar_list_xbrl_xml.py     -y ${YEAR}
    python3 sec_edgar_download_xbrl_xml.py -y ${YEAR}
    python3 sec_edgar_parse_xbrl_xml.py    -y ${YEAR}

    YEAR=$((${YEAR}+1))
done

# copy generated data to the bucket
# Make sure to setup the AWS account key/credential for AWS CLI.
# Set the region to us-east-1 or set AWS_DEFAULT_REGION.
# [default]
#   #region = us-east-1
S3_URL="s3://oonisim-sec-edgar/"
aws s3 sync ../data/ $S3_URL