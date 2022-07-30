#!/usr/bin/env bash
#--------------------------------------------------------------------------------
# Download EDGAR XBRL master index files.
# https://www.sec.gov/Archives/edgar/full-index stores the master index files for
# filings in XBRL format as well as those in HTML format.
#
# Each year/quarter has its own master index file with the URL format:
# https://www.sec.gov/Archives/edgar/full-index/${YEAR}/QTR{$QTR}/xbrl.gz".
#
# We download the master indices for XBRL, not HTML per year/qtr.
#
# [NOTE]
# There are two types of master index file, one for XBRL and the other for HTML.
# Because initially EDGAR did not use XBRL or not mandatory, hence the documents are
# previously in HTML format and the TXT is the all-in-one including all the filing data
# sectioned by <DOCUMENT>.

# [xbrl.gz] master index file for TXT file in XBRL version
# |CIK    |Company Name|Form Type|Filing Date|TXT Path                                   |
# |1002047|NetApp, Inc.|10-Q     |2020-02-18 |edgar/data/1002047/0001564590-20-005025.txt|
#
# [master.gz] master index file for  TXT file in HTML version
# |CIK   |Company Name|Form Type|Filing Date|TXT Path                                   |
# |000180|SANDISK CORP|10-Q      |2006-08-10|edgar/data/1000180/0000950134-06-015727.txt|
#
#--------------------------------------------------------------------------------
set -e
if $# -lt 1 ; then
  echo "Usage: $(basename -- "$0") <year>"
  exit -1
fi
YEAR_START=$1
YEAR_END=$(date "+%Y")

DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${DIR}

DATA_DIR=$(realpath ../data/csv/index)
mkdir -p ${DATA_DIR}


EDGAR_MASTER_INDEX_BASE_URL="https://www.sec.gov/Archives/edgar/full-index"
YEAR=${YEAR_START}
while [ ${YEAR} -le ${YEAR_END} ]
do
    QTR=1
    while [ ${QTR} -le 4 ]
    do
        echo "Downloading the XBRL master index file for ${YEAR} QTR ${QTR}..."
        curl --silent -C - \
        --header 'User-Agent:Company Name myname@company.com' \
        --output "${DATA_DIR}/${YEAR}QTR${QTR}.gz" \
        "${EDGAR_MASTER_INDEX_BASE_URL}/${YEAR}/QTR{$QTR}/xbrl.gz"

        sleep 1
        QTR=$((${QTR}+1))
    done

    YEAR=$((${YEAR}+1))
done

cd ${DATA_DIR}
for gz in $(ls *.gz)
do
  if gzip -t ${gz} ; then
      gunzip -f ${gz}
      # Remove non-csv lines
      sed -i -e '1,/^[ \t]*$/d; /^[- \t]*$/d' $(basename ${gz} .gz)
  else
      echo "${gz} is not a valid gzip file. Verify the year/qtr is valid"
  fi
done