#!/usr/bin/env bash
YEAR=2022
QTR=1

rm -f ../data/csv/listing/${YEAR}QTR${QTR}_LIST.gz
rm -f ../data/csv/xbrl/${YEAR}QTR${QTR}_XBRL.gz
rm -f ../data/csv/gaap/${YEAR}QTR${QTR}_GAAP.gz

python3 sec_edgar_list_xbrl_xml.py      -y ${YEAR} -q ${QTR} -t -l 20 -n 1
python3 sec_edgar_download_xbrl_xml.py  -y ${YEAR} -q ${QTR} -t -l 20 -n 1
python3 sec_edgar_parse_xbrl_xml.py     -y ${YEAR} -q ${QTR} -t -l 20 -n 1
