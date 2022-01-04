#!/usr/bin/env python
"""
## Objective
Navigate through the EDGAR XBRL listings to generate the URLs to XBRL XML files.

## XBRL master index file
https://www.sec.gov/Archives/edgar/full-index stores the master index files.
[xbrl.gz] master index file for  TXT file in XBRL version

[Format]
|CIK    |Company Name|Form Type|Filing Date|TXT Path                                   |
|-------|------------|---------|-----------|-------------------------------------------|
|1002047|NetApp, Inc.|10-Q     |2020-02-18 |edgar/data/1002047/0001564590-20-005025.txt|

[Background]
Previously, XBRL was not introduced or not mandatory, hence the documents are
in HTML format and the TXT is the all-in-one including all the filing data
sectioned by <DOCUMENT>.

After the introduction of XBRL and recent mandates, the XBRL master index points
to the TXT file which is al-in-one including all the filing data in XML format. 
However, it is not straight-forward to parse the TXT for XBRL and there is
.xml file which has all the XML data only.

Hence, need to identify the URL to XBRL XML. However, the format fo the filename
is not consistent, e.g. <name>_htm.xml, <name>.xml. Needs to identify the XML
filename, then create the UR.

## EDGAR Directory Listing
Each filing directory provides the listing which lists all the files in the
directory either as index.html, index.json, or index.xml. Parse the index.xml
to identify the XBRL XML file.

https://sec.gov/Archives/edgar/full-index/${YEAR}/${QTR}/${ACCESSION}/index.xml

# EDGAR
* Investopedia -[Where Can I Find a Company's Annual Report and Its SEC Filings?](https://www.investopedia.com/ask/answers/119.asp)

> If you want to dig deeper and go beyond the slick marketing version of the annual report found on corporate websites, you'll have to search through required filings made to the Securities and Exchange Commission. All publicly-traded companies in the U.S. must file regular financial reports with the SEC. These filings include the annual report (known as the 10-K), quarterly report (10-Q), and a myriad of other forms containing all types of financial data.45

# Quarterly filing indices
* [Accessing EDGAR Data](https://www.sec.gov/os/accessing-edgar-data)

> Using the EDGAR index files  
Indexes to all public filings are available from 1994Q3 through the present and located in the following browsable directories:
> * https://www.sec.gov/Archives/edgar/daily-index/ — daily index files through the current year; (**DO NOT forget the trailing slash '/'**)
> * https://www.sec.gov/Archives/edgar/full-index/ — full indexes offer a "bridge" between quarterly and daily indexes, compiling filings from the beginning of the current quarter through the previous business day. At the end of the quarter, the full index is rolled into a static quarterly index.
> 
> Each directory and all child sub directories contain three files to assist in automated crawling of these directories. Note that these are not visible through directory browsing.
> * index.html (the web browser would normally receive these)
> * index.xml (an XML structured version of the same content)
> * index.json (a JSON structured vision of the same content)
> 
> Four types of indexes are available:
> * company — sorted by company name
> * form — sorted by form type
> * master — sorted by CIK number 
> * **XBRL** — list of submissions containing XBRL financial files, sorted by CIK number; these include Voluntary Filer Program submissions
> 
> The EDGAR indexes list the following information for each filing:
> * company name
> * form type
> * central index key (CIK)
> * date filed
> * file name (including folder path)

## Example

Full index files for 2006 QTR 3.
<img src="../image/edgar_full_index_quarter_2006QTR3.png" align="left" width="800"/>
"""
# ================================================================================
# Setup
# ================================================================================
import argparse
import logging
import os
import pathlib
import random
import re

import Levenshtein as levenshtein
import bs4
import pandas as pd
import ray
import time
from bs4 import (
    BeautifulSoup
)

from sec_edgar_common import (
    http_get_content,
    split,
    list_csv_files,
    load_from_csv,
    director,
    output_filepath_for_input_filepath,
)
from sec_edgar_constant import (
    TEST_MODE,
    NUM_CPUS,
    SEC_FORM_TYPE_10K,
    SEC_FORM_TYPE_10Q,
    EDGAR_BASE_URL,
    EDGAR_HTTP_HEADERS,
    DEFAULT_LOG_LEVEL,
    DIR_DATA_CSV_INDEX,
    DIR_DATA_CSV_LIST
)

pd.set_option('display.max_colwidth', None)


# ================================================================================
# Utilities
# ================================================================================
def input_csv_suffix_default():
    return ""


def output_csv_suffix_default():
    return "_LIST.gz"


def get_command_line_arguments():
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description='argparse test program')
    parser.add_argument(
        '-ic', '--input-csv-directory', type=str, required=False, default=DIR_DATA_CSV_INDEX,
        help='specify the input csv directory'
    )
    parser.add_argument(
        '-is', '--input-csv-suffix', type=str, required=False, default=input_csv_suffix_default(),
        help=f'specify the input filename suffix e.g {input_csv_suffix_default()}'
    )
    parser.add_argument(
        '-oc', '--output-csv-directory', type=str, required=False, default=DIR_DATA_CSV_LIST,
        help='specify the output csv directory'
    )
    parser.add_argument(
        '-os', '--output-csv-suffix', type=str, required=False, default=output_csv_suffix_default(),
        help=f'specify the output csv filename suffix e.g {output_csv_suffix_default()}'
    )
    parser.add_argument(
        '-y', '--year', type=int, required=False, help='specify the target year'
    )
    parser.add_argument(
        '-q', '--qtr', type=int, choices=[1, 2, 3, 4], required=False,
        help='specify the target quarter'
    )
    parser.add_argument(
        '-n', '--num-workers', type=int, required=False,
        help='specify the number of workers to use'
    )
    parser.add_argument(
        '-l', '--log-level', type=int, choices=[10, 20, 30, 40], required=False,
        default=DEFAULT_LOG_LEVEL,
        help='specify the logging level (10 for INFO)',
    )
    args = vars(parser.parse_args())

    # Mandatory
    input_csv_directory = args['input_csv_directory']
    input_csv_suffix = args['input_csv_suffix']
    output_csv_directory = args['output_csv_directory']
    output_csv_suffix = args['output_csv_suffix']

    # Optional
    year = str(args['year']) if args['year'] else None
    qtr = str(args['qtr']) if args['qtr'] else None
    num_workers = args['num_workers'] if args['num_workers'] else NUM_CPUS
    log_level = args['log_level'] if args['log_level'] else DEFAULT_LOG_LEVEL

    return input_csv_directory, \
           input_csv_suffix, \
           output_csv_directory, \
           output_csv_suffix, \
           year, \
           qtr, \
           num_workers, \
           log_level


def csv_absolute_path_to_save(directory, basename, suffix="_LIST.gz"):
    """Generate the absolute path to the directory in which to save the csv file
    Args:
        directory: Absolute path to the directory to save the file.
        basename: Filename of the file to save
        suffix: Suffix of the filename e.g. _LIST.gz.
    """
    if not hasattr(csv_absolute_path_to_save, "mkdired"):
        pathlib.Path(directory).mkdir(mode=0o775, parents=True, exist_ok=True)
        setattr(csv_absolute_path_to_save, "mkdired", True)

    destination = os.path.realpath(f"{directory}{os.sep}{basename}{suffix}")
    logging.debug("csv_absolute_path_to_save(): Path to save csv is [%s]" % destination)
    return destination


def input_filename_pattern(year, qtr, suffix):
    """Generate glob pattern to find the input files"""
    pattern = ""
    pattern += f"{year}" if year else "*"
    pattern += "QTR"
    pattern += f"{qtr}" if qtr else "?"
    pattern += suffix if suffix is not None else ""
    return pattern


# ================================================================================
# Logic
# ================================================================================
def index_xml_url(filename):
    """
    Generate the URL to the EDGAR directory listing index.xml file in the format
    'https://sec.gov/Archives/edgar/data/{CIK}/{ACCESSION}/index.xml' from the
    directory path to the XBRL TXT file specified in the EDGAR XBRL index CSV.

    | CIK    | Company Name | Form Type          | Date Filed | Filename   |
    |--------|--------------|--------------------|------------|------------|
    |1047127 |AMKOR         |10-K                |2014-02-28  |edgar/data/1047127/0001047127-14-000006.txt|

    Example: From 'edgar/data/1001039/0001193125-10-025949.txt', generate
    'https://sec.gov/Archives/edgar/data/1001039/000119312510025949/index.xml'
    """
    # --------------------------------------------------------------------------------
    # The index_xml_url can be None
    # --------------------------------------------------------------------------------
    if filename is None or filename == "":
        logging.warning("index_xml_url(): skip invalid filename [%s]" % filename)
        return None

    # --------------------------------------------------------------------------------
    # Verify the filename has the "edgar/data/{CIK}/{ACCESSION_NUMBER}.txt" format
    # where ACCESSION_NUMBER is like 0001047127-14-000006.
    # --------------------------------------------------------------------------------
    expected = "edgar/data/{CIK}/{ACCESSION_NUMBER}.txt"
    pattern = r"edgar/data/[0-9]+/[0-9]+[-0-9]+\.txt"
    if re.match(pattern, filename):
        # --------------------------------------------------------------------------------
        # Generate URL to the directory listing index.xml for the ACCESSION_NUMBER
        # The result URL format is "http://sec.gov/edgar/data/{CIK}/{ACCESSION}/index.xml
        # where ACCESSION is created by reoving '-' from ACCESSION_NUMBER.
        # --------------------------------------------------------------------------------
        url = "/".join([
            EDGAR_BASE_URL,
            filename.rstrip(".txt").replace('-', ''),
            "index.xml"
        ])
    else:
        logging.error("index_xml_url(): expected the format [{}] but got [{}]".format(expected, filename))
        url = None

    return url


def filing_directory_path(index_xml_url: str):
    """
    Get the SEC Filing directory path "/Archives/edgar/data/{CIK}/{ACCESSION}/"
    from the URL to EDGAR directory listing index.xml by removing the base
    "https://[www.]sec.gov" and filename "index.xml".

    Args:
        index_xml_url: URL to the EDGAR directory listing index.xml
    Returns:
        "/Archives/edgar/data/{CIK}/{ACCESSION}/" part in the index_xml_url
    """
    # --------------------------------------------------------------------------------
    # regexp pattern to extract the directory path part (3rd group)
    # --------------------------------------------------------------------------------
    pattern = r"(http|https)://(www\.sec\.gov|sec\.gov)(.*/)index.xml"
    match = re.search(pattern, index_xml_url, re.IGNORECASE)

    # --------------------------------------------------------------------------------
    # Verify the match
    # --------------------------------------------------------------------------------
    assert match and match.group(3) and re.match(r"^/Archives/edgar/data/[0-9]*/[0-9]*/", match.group(3)), \
        f"regexp [{pattern}] found No matching directory path in url {index_xml_url}.\n{match}"

    path = match.group(3)
    logging.debug("Filing directory path is [%s]" % path)
    return path


def find_xbrl_url_from_html_xml(source, directory):
    """Find XBRL XML with suffix _htm.xml in the Filing directory
    Args:
        source: Content of filing directory listing index.xml (as BS4)
        directory: Filing directory path "/Archives/edgar/data/{CIK}/{ACCESSION}/"
    Returns:
        URL to the XBRL XML if found, else None
    """
    assert isinstance(source, bs4.BeautifulSoup)
    path_to_xbrl = source.find('href', string=re.compile(".*_htm\.xml"))
    if path_to_xbrl:
        url = "https://sec.gov" + path_to_xbrl.string.strip()
        logging.info("XBRL XML [%s] identified" % url)
        return url
    else:
        logging.warning("No XBRL XML pattern [%s.*_htm,.xml] identified." % directory)
        return None


def find_xbrl_url_from_xsd(source, directory):
    """Find XBRL XML having similar name with .xsd file in the Filing directory.
    XBRL XML requires XSD file and the XBRL is likely to have the same file name
    of the XSD file, e.g. <filename>.xml and <filename>.xsd.

    <filename> does not always 100% match, e.g. asc-20191223.xsd and asc-20190924.
    Hence, use the LEVENSHTEIN DISTANCE to fuzzy match.

    Args:
        source: Content of filing directory listing index.xml (as BS4)
        directory: Filing directory path "/Archives/edgar/data/{CIK}/{ACCESSION}/"
    Returns:
        URL to XBRL XML if found, else None
    """
    assert isinstance(source, bs4.BeautifulSoup)
    MAX_LEVENSHTEIN_DISTANCE = 5

    path_to_xsd = source.find('href', string=re.compile(re.escape(directory) + ".*\.xsd"))
    if path_to_xsd:
        # Extract filename from "/Archives/edgar/data/{CIK}/{ACCESSION}/<filename>.xsd".        
        pattern = re.escape(directory) + r"(.*)\.xsd"
        path_to_xsd = path_to_xsd.string.strip()
        match = re.search(pattern, path_to_xsd, re.IGNORECASE)
        assert match and match.group(1), f"No filename match for with {pattern}"

        # Filename of the XSD
        filename = match.group(1)

        # Iterate over all .xml files and find the distance from the XSD filename.
        distance = 999
        candidate = None
        for href in source.find_all('href', string=re.compile(re.escape(directory) + ".*\.xml")):
            pattern = re.escape(directory) + r"(.*)\.xml"
            match = re.search(pattern, href.string.strip(), re.IGNORECASE)
            assert match and match.group(1), f"[{href}] has no .xml with {pattern}"

            potential = match.group(1)
            new_distance = levenshtein.distance(filename, potential)
            if new_distance < distance:
                distance = new_distance
                candidate = potential
                logging.debug(
                    "Candidate [%s] is picked with the distance from [%s] is [%s]."
                    % (candidate, filename, distance)
                )

        # Accept within N-distance away from the XSD filename.
        if distance < MAX_LEVENSHTEIN_DISTANCE:
            path_to_xml = directory + candidate.strip() + ".xml"
            url = "https://sec.gov" + path_to_xml
            logging.debug(
                "Selected the candidate [%s] of distance [%s]. \nURL to XBRL is [%s]"
                % (candidate, distance, url)
            )
            logging.info("XBRL XML [%s] identified" % url)
            return url
        else:
            logging.warning(
                "No XBRL XML identified from the XSD file [%s%s]."
                % (directory, filename + ".xsd")
            )
            return None
    else:
        logging.error("No XBRL identified from XSD [%s]." % path_to_xsd)
        return None


def find_xbrl_url_from_auxiliary_regexp(source, directory):
    """Find XBRL XML with regexp pattern.
    XBRL XML file most likely has the format if <str>-<YEAR><MONTH><DATE>.xml,
    but should NOT be Rnn.xml e.g. R1.xml or R10.xml.

    Args:
        source: Content of filing directory listing index.xml (as BS4)
        directory: Filing directory path "/Archives/edgar/data/{CIK}/{ACCESSION}/"
    Returns:
        URL to the XBRL XML if found, else None
    """
    assert isinstance(source, bs4.BeautifulSoup)
    regexp = re.escape(directory) + r"[^R][a-zA-Z_-]*[0-9][0-9][0-9][0-9][0-9].*\.xml"
    logging.debug("Look for XBRL XML with the regexp [%s]." % regexp)

    path_to_xbrl = source.find('href', string=re.compile(regexp))
    if path_to_xbrl:
        url = "https://sec.gov" + path_to_xbrl.string.strip()
        logging.info("XBRL XML [%s] identified" % url)
        return url
    else:
        logging.warning(
            "No XBRL XML identified with the regexp [%s] in [%s]." % (regexp, directory)
        )
        return None


def xbrl_url(index_xml_url: str):
    """Generate the URL to the XBML file in the filing directory 
    Args:
        index_xml_url: 
            URL to the EDGAR directory listing index.xml file whose format is e.g.:
            "https://sec.gov/Archives/edgar/data/62996/000095012310013437/index.xml"
    Returns:
        URL to the XBRL file in the filing directory, or None
    """
    # --------------------------------------------------------------------------------
    # The index_xml_url can be None
    # --------------------------------------------------------------------------------
    if index_xml_url is None or index_xml_url == "":
        logging.warning("xbrl_url(): skip invalid index_xml_url [%s]" % index_xml_url)
        return None

    # --------------------------------------------------------------------------------
    # URL to directory listing index.xml
    # --------------------------------------------------------------------------------
    index_xml_url = index_xml_url.strip()
    max_retries_allowed = 3
    while True:
        try:
            # --------------------------------------------------------------------------------
            # https://www.sec.gov/oit/announcement/new-rate-control-limits
            # If a user or application submits more than 10 requests per second to EDGAR websites,
            # SEC may limit further requests from the relevant IP address(es) for a brief period.
            #
            # TODO:
            #  Synchronization among workers to limit the rate 10/sec from the same IP.
            #  For now, just wait 1 sec at each invocation from the worker.
            # --------------------------------------------------------------------------------
            time.sleep(1)

            logging.info("xbrl_url(): getting filing directory index [%s]..." % index_xml_url)
            content = http_get_content(index_xml_url, EDGAR_HTTP_HEADERS)
            break
        except RuntimeError as e:
            max_retries_allowed -= 1
            if max_retries_allowed > 0:
                logging.error("xbrl_url(): failed to get [%s]. retrying..." % index_xml_url)
                time.sleep(random.randint(30, 90))
            else:
                content = None
                logging.error("xbrl_url(): failed to get [%s]. skipping..." % index_xml_url)
                break

    if content is not None:
        logging.info(f"identifying XBRL URL for [%s]..." % index_xml_url)
        # --------------------------------------------------------------------------------
        # "/Archives/edgar/data/{CIK}/{ACCESSION}/" part of the EDGAR filing URL
        # --------------------------------------------------------------------------------
        directory = filing_directory_path(index_xml_url)

        # --------------------------------------------------------------------------------
        # Look for the XBRL XML file in the index.xml.
        # 1. _htm.xml file
        # 2. <filename>.xml where "filename" is from <filename>.xsd.
        # 3. <filename>.xml where "filename" is not RNN.xml e.g. R10.xml.
        # --------------------------------------------------------------------------------
        source = BeautifulSoup(content, 'html.parser')
        url = find_xbrl_url_from_html_xml(source=source, directory=directory)
        if url:
            logging.info(f"XBRL URL identified [%s]" % url)
            return url

        url = find_xbrl_url_from_xsd(source, directory=directory)
        if url:
            logging.info(f"XBRL URL identified [%s]" % url)
            return url

        url = find_xbrl_url_from_auxiliary_regexp(source=source, directory=directory)
        if url:
            logging.info(f"XBRL URL identified [%s]" % url)
            return url

        logging.error("No XBRL identified in the listing [%s]" % index_xml_url)
        return None
    else:
        return None


@ray.remote(num_returns=1)
def worker(msg):
    """GET XBRL XML and save to a file.
    Args:
        msg: message
    Returns: Pandas dataframe where "Filename" column is updated with XBRL XML URL.
    """
    df = msg["data"]
    log_level:int = msg['log_level']
    assert df is not None and len(df) > 0, "worker(): invalid dataframe"
    assert log_level in [10, 20, 30, 40]

    # --------------------------------------------------------------------------------
    #  Logging setup for Ray as in https://docs.ray.io/en/master/ray-logging.html.
    #  In Ray, all of the tasks and actors are executed remotely in the worker processes.
    #  Since Python logger module creates a singleton logger per process, loggers should
    #  be configured on per task/actor basis.
    # --------------------------------------------------------------------------------
    logging.basicConfig(level=log_level)
    logging.info("worker(): task size is %s" % len(df))

    # --------------------------------------------------------------------------------
    # Update the 'Filename' column with the URL to index.xml
    # --------------------------------------------------------------------------------
    # df.loc[:, 'Filename'] = df['Filename'].apply(index_xml_url)
    # df.loc[:, 'Filename'] = df['Filename'].apply(xbrl_url)
    df.loc[:, 'Filename'] = df['Filename'].apply(lambda txt: xbrl_url(index_xml_url(txt)))
    return df


def dispatch(msg: dict):
    filepath = msg['filepath']
    num_workers = msg['num_workers']
    log_level = msg['log_level']

    # --------------------------------------------------------------------------------
    # Load the listing CSV ({YEAR}QTR{QTR}_LIST.gz) into datafame
    # --------------------------------------------------------------------------------
    df = load_from_csv(filepath=filepath, types=[SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K])
    assert df is not None and len(df) > 0, f"Invalid dataframe \n{df}"
    if TEST_MODE:
        df = df.head(NUM_CPUS)

    # --------------------------------------------------------------------------------
    # Asynchronously invoke tasks
    # --------------------------------------------------------------------------------
    futures = [
        worker.remote({
            "data": task,
            "log_level": log_level
        })
        for task in split(tasks=df, num=num_workers)
    ]
    assert len(futures) == num_workers, f"Expected {num_workers} tasks but got {len(futures)}."
    return futures


# --------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    # --------------------------------------------------------------------------------
    # Command line arguments
    # --------------------------------------------------------------------------------
    args = get_command_line_arguments()
    (
        input_csv_directory,
        input_csv_suffix,
        output_csv_directory,
        output_csv_suffix,
        year,
        qtr,
        num_workers,
        log_level
    ) = args

    # --------------------------------------------------------------------------------
    # Logging
    # --------------------------------------------------------------------------------
    logging.basicConfig(level=log_level)

    # --------------------------------------------------------------------------------
    # XBRL XML logic
    # --------------------------------------------------------------------------------
    try:
        # --------------------------------------------------------------------------------
        # Setup Ray
        # --------------------------------------------------------------------------------
        logging.info("main(): initializing Ray using %s workers..." % num_workers)
        ray.init(num_cpus=num_workers, num_gpus=0, logging_level=log_level)

        # --------------------------------------------------------------------------------
        # Process XBRL listing files
        # --------------------------------------------------------------------------------
        for filepath in list_csv_files(
                input_csv_directory=input_csv_directory,
                input_filename_pattern=input_filename_pattern(year, qtr, input_csv_suffix),
                f_output_filepath_for_input_filepath=output_filepath_for_input_filepath(
                    output_csv_directory, output_csv_suffix, input_csv_suffix
                )
        ):
            filename = os.path.basename(filepath)
            logging.info("main(): processing the master index csv [%s]..." % filename)

            # --------------------------------------------------------------------------------
            # Year/Quarter of the listing is filed to SEC
            # --------------------------------------------------------------------------------
            match = re.search("^([1-2][0-9]{3})QTR([1-4]).*$", filename, re.IGNORECASE)
            year = match.group(1)
            qtr = match.group(2)
            basename = f"{year}QTR{qtr}"

            # --------------------------------------------------------------------------------
            # Process the single index file
            # --------------------------------------------------------------------------------
            msg = {
                "filepath": filepath,
                "year": year,
                "qtr": qtr,
                "output_csv_directory": output_csv_directory,
                "output_csv_suffix": output_csv_suffix,
                "basename": basename,
                "num_workers": num_workers,
                "log_level": log_level
            }
            df = director(msg, dispatch, csv_absolute_path_to_save)

            # --------------------------------------------------------------------------------
            # List failed records with 'Filepath' column being None as failed to get XBRL
            # --------------------------------------------------------------------------------
            if any(df['Filename'].isna()):
                print("*" * 80)
                print("-" * 80)
                print(f"[{len(df['Filename'].isna())}] failed records:\n")
                print(df[df['Filename'].isna()])
            else:
                logging.info("main(): all [%s] records processed in %s" % (len(df), filename))

    finally:
        # --------------------------------------------------------------------------------
        # Clean up resource
        # --------------------------------------------------------------------------------
        logging.info("main(): shutting down Ray...")
        ray.shutdown()

