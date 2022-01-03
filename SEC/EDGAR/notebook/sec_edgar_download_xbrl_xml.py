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
* Investopedia -[Where Can I Find a Company's Annual Report and Its SEC Filings?]
(https://www.investopedia.com/ask/answers/119.asp)

> If you want to dig deeper and go beyond the slick marketing version of the annual
report found on corporate websites, you'll have to search through required filings
made to the Securities and Exchange Commission. All publicly-traded companies in
the U.S. must file regular financial reports with the SEC. These filings include
the annual report (known as the 10-K), quarterly report (10-Q), and a myriad of
other forms containing all types of financial data.45

# Quarterly filing indices
* [Accessing EDGAR Data](https://www.sec.gov/os/accessing-edgar-data)


## Example

Full index files for 2006 QTR 3.
<img src="../image/edgar_full_index_quarter_2006QTR3.png" align="left" width="800"/>
"""
# ================================================================================
# Setup
# ================================================================================
import argparse
import gzip
import logging
import os
import pathlib
import random
import re
import time

import pandas as pd
import ray

from sec_edgar_common import (
    filename_extension,
    http_get_content,
    split,
    list_csv_files,
    load_from_csv,
    output_filepath_for_input_filepath,
    director,
)
from sec_edgar_constant import (
    TEST_MODE,
    NUM_CPUS,
    SEC_FORM_TYPE_10K,
    SEC_FORM_TYPE_10Q,
    EDGAR_HTTP_HEADERS,
    DEFAULT_LOG_LEVEL,
    DIR_DATA_CSV_LIST,
    DIR_DATA_CSV_XBRL,
    DIR_DATA_XML_XBRL,
)

pd.set_option('display.max_colwidth', None)


# ================================================================================
# Utilities
# ================================================================================
def input_csv_suffix_default():
    return "_LIST.gz"


def output_csv_suffix_default():
    return "_XBRL.gz"


def get_command_line_arguments():
    """Get command line arguments"""
    parser = argparse.ArgumentParser(description='argparse test program')
    parser.add_argument(
        '-ic', '--input-csv-directory', type=str, required=False, default=DIR_DATA_CSV_LIST,
        help='specify the input data directory'
    )
    parser.add_argument(
        '-is', '--input-csv-suffix', type=str, required=False, default=input_csv_suffix_default(),
        help=f'specify the input filename suffix e.g {input_csv_suffix_default()}'
    )
    parser.add_argument(
        '-oc', '--output-csv-directory', type=str, required=False, default=DIR_DATA_CSV_XBRL,
        help='specify the output data directory to save the csv file (not xml)'
    )
    parser.add_argument(
        '-os', '--output-csv-suffix', type=str, required=False, default=output_csv_suffix_default(),
        help=f'specify the output csv filename suffix e.g {output_csv_suffix_default()}'
    )
    parser.add_argument(
        '-ox', '--output-xml-directory', type=str, required=False, default=DIR_DATA_XML_XBRL,
        help='specify the output data directory to save the xml file (not csv)'
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
    output_xml_directory = args['output_xml_directory']

    # Optional
    year = str(args['year']) if args['year'] else None
    qtr = str(args['qtr']) if args['qtr'] else None
    num_workers = args['num_workers'] if args['num_workers'] else NUM_CPUS
    log_level = args['log_level'] if args['log_level'] else DEFAULT_LOG_LEVEL

    return input_csv_directory, \
        input_csv_suffix, \
        output_csv_directory, \
        output_csv_suffix, \
        output_xml_directory, \
        year, \
        qtr, \
        num_workers, \
        log_level


def xml_relative_path_to_save(directory, basename):
    """
    Generate the relative file path from the output_xml_directory to save the XBRL XML.
    XML is saved to {output_xml_directory}/{directory}/{basename}.gz.
    The function returns {directory}/{basename}.gz part.

    Args:
        directory: location to save the file
        basename: basename of the file to save
    Returns: Absolute file path
    """
    assert not directory.startswith(os.sep), "Must be relative directory path"
    relative = f"{directory}{os.sep}{basename}.gz"
    return relative


def xml_absolute_path_to_save(output_xml_directory, directory, basename):
    """
    Generate the absolute file path to save the XBRL XML. Create directory if required.
    Each XML is saved to {output_xml_directory}/{directory}/{basename}.gz.

    Args:
        output_xml_directory: Base directory for XML
        directory: Relative path from the output_xml_directory
        basename: basename of the file to save
    Returns: Absolute file path
    """
    relative = xml_relative_path_to_save(directory, basename)
    absolute = os.path.realpath(f"{output_xml_directory}{os.sep}{relative}")
    pathlib.Path(os.path.dirname(absolute)).mkdir(mode=0o775, parents=True, exist_ok=True)
    return absolute


def csv_absolute_path_to_save(directory, basename, suffix=""):
    """
    Generate the absolute file path to save the dataframe as csv. Create directory if required.

    Args:
        directory: *Absolute* path to the directory to save the file
        basename: Name of the file to save

    Returns: Absolute file path
    """
    if not hasattr(csv_absolute_path_to_save, "mkdired"):
        pathlib.Path(directory).mkdir(mode=0o775, parents=True, exist_ok=True)
        setattr(csv_absolute_path_to_save, "mkdired", True)

    absolute = os.path.realpath(f"{directory}{os.sep}{basename}{suffix}")
    return absolute


def input_filename_pattern(year, qtr, suffix):
    """Generate glob pattern to find the input files"""
    pattern = ""
    pattern += f"{year}" if year else "*"
    pattern += "QTR"
    pattern += f"{qtr}" if qtr else "?"
    pattern += suffix if suffix is not None else ""
    return pattern


def get_xml(url):
    """GET XBRL XML from the URL
    Args:
        url: URL to download the XBRL XML
    Returns: XBRL XML content or None
    """
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

            logging.info("get_xml(): getting XBRL XML [%s]..." % url)
            content = http_get_content(url, EDGAR_HTTP_HEADERS)
            return content
        except RuntimeError as e:
            max_retries_allowed -= 1
            if max_retries_allowed > 0:
                logging.error("get_xml(): failed to get [%s]. retrying..." % url)
                time.sleep(random.randint(30, 90))
            else:
                logging.error("get_xml(): failed to get [%s]. skipping..." % url)
                break

    return None


def save_to_xml(msg):
    """Save the XBRL XML to the path relative to DIR_DATA_XML_XBRL.

    Args:
        msg: message including the content data, directory to save it as filename
    Returns: relative path to the XML file if successfully saved, else None
    """
    content = msg["data"]
    output_xml_directory = msg['output_xml_directory']
    directory: str = msg['directory']
    basename: str = msg['basename']
    assert not directory.startswith("/"), f"Must not start with '/' but [{directory}]"

    destination = xml_absolute_path_to_save(
        output_xml_directory=output_xml_directory, directory=directory, basename=basename
    )
    extension = filename_extension(destination)

    logging.info("save_to_xml(): saving XBRL XML to [%s]..." % destination)
    try:
        if extension == ".gz":
            with gzip.open(f"{destination}", 'wb') as f:
                f.write(content.encode())
        elif extension == "":
            with open(destination, "w") as f:
                f.write(content.encode)
        else:
            assert False, f"Unknown file type [{extension}]"
    except IOError as e:
        logging.error("save_to_xml(): failed to save [%s] due to [%s]" % (destination, e))
        return None
    else:
        return xml_relative_path_to_save(directory, basename)


# ================================================================================
# Logic
# ================================================================================
@ray.remote(num_returns=1)
def worker(msg:dict):
    """
    1. GET XBRL XML from the filing directory and save to a file.
    2. Update the listing dataframe with the year, qtr, path to the saved XBRL XML.
       Set None to the path column when failed to get the XBRL XML.

    The incoming dataframe has the format where 'Filename' is the URL to XBRL XML
    in the filing directory.
    |CIK|Company Name|Form Type|Date Filed|Filename|

    Args:
        msg: Dictionary to data package of format {
                "data": <dataframe>,
                "year": <year of the filing>,
                "qtr": <quarter of the filing>,
                "log_level": <logging level>
        }

    Returns: Updated dataframe
    """
    df = msg["data"]
    year: str = msg['year']
    qtr: str = msg['qtr']
    output_xml_directory = msg["output_xml_directory"]
    log_level:int = msg['log_level']

    # --------------------------------------------------------------------------------
    #  Logging setup for Ray as in https://docs.ray.io/en/master/ray-logging.html.
    #  In Ray, all of the tasks and actors are executed remotely in the worker processes.
    #  Since Python logger module creates a singleton logger per process, loggers should
    #  be configured on per task/actor basis.
    # --------------------------------------------------------------------------------
    assert log_level in [10, 20, 30, 40]
    logging.basicConfig(level=log_level)
    logging.info("worker(): task size is %s" % len(df))

    # --------------------------------------------------------------------------------
    # Add year/qtr/filepath columns
    # --------------------------------------------------------------------------------
    assert year.isdecimal() and re.match(r"^[12][0-9]{3}$", year)
    assert qtr.isdecimal() and re.match(r"^[1-4]$", qtr)
    df.insert(loc=3, column='Year', value=pd.Categorical([year]* len(df)))
    df.insert(loc=4, column='Quarter', value=pd.Categorical([qtr]* len(df)))
    df.insert(loc=len(df.columns), column="Filepath", value=[None]*len(df))

    for index, row in df.iterrows():
        # --------------------------------------------------------------------------------
        # Download XBRL XML
        # --------------------------------------------------------------------------------
        url = row['Filename']
        content = get_xml(url)

        if content:
            # --------------------------------------------------------------------------------
            # Save XBRL XML
            # URL format: https://sec.gov/Archives/edgar/data/{cik}}/{accession}}/{filename}
            # https://sec.gov/Archives/edgar/data/1000697/000095012310017583/wat-20091231.xml
            # --------------------------------------------------------------------------------
            elements = url.split('/')
            basename = elements[-1]
            accession = elements[-2]
            cik = elements[-3]
            assert str(row['CIK']) == cik, \
                f"CIK [{row['CIK']})] must match CIK part [{cik}] in url {url}"

            # Note: The directory is relative path, NOT absolute
            directory = f"{cik}{os.sep}{accession}"
            package = {
                "data": content,
                "output_xml_directory": output_xml_directory,
                "directory": directory,
                "basename": basename
            }
            path_to_saved_xml = save_to_xml(package)

            # --------------------------------------------------------------------------------
            # Update the dataframe with the filepath where the XBRL XML has been saved.
            # --------------------------------------------------------------------------------
            if path_to_saved_xml:
                df.at[index, 'Filepath'] = path_to_saved_xml
                logging.debug(
                    "worker(): updated the dataframe[%s, 'Filepath'] with [%s]."
                    % (index, path_to_saved_xml)
                )
            else:
                logging.debug(
                    "worker(): not updated the dataframe[%s, 'Filepath'] as saving has failed."
                    % index
                )
        else:
            logging.debug(
                "worker(): not updated the dataframe[%s, 'Filepath'] as failed to get XBRL XML"
                % index
            )

    return df


def dispatch(msg: dict):
    filepath = msg['filepath']
    year = msg['year']
    qtr = msg['qtr']
    output_xml_directory = msg["output_xml_directory"]
    num_workers = msg['num_workers']
    log_level = msg['log_level']

    # --------------------------------------------------------------------------------
    # Load the listing CSV ({YEAR}QTR{QTR}_LIST.gz) into datafame
    # --------------------------------------------------------------------------------
    df = load_from_csv(filepath=filepath, types=[SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K])
    assert df is not None and len(df) > 0, "worker(): invalid dataframe"
    if TEST_MODE:
        df = df.head(NUM_CPUS)

    # --------------------------------------------------------------------------------
    # Asynchronously invoke tasks
    # --------------------------------------------------------------------------------
    futures = [
        worker.remote({
            "data": task,
            "year": year,
            "qtr": qtr,
            "output_xml_directory": output_xml_directory,
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
        output_xml_directory,
        year,
        qtr,
        num_workers,
        log_level
    ) = args

    # --------------------------------------------------------------------------------
    # Logging
    # --------------------------------------------------------------------------------
    logging.basicConfig(level=log_level)
    # logging.addHandler(logging.StreamHandler())

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
            logging.info("main(): processing the listing csv [%s]..." % filename)

            # --------------------------------------------------------------------------------
            # Year/Quarter of the listing is filed to SEC
            # --------------------------------------------------------------------------------
            match = re.search("^([1-2][0-9]{3})QTR([1-4]).*$", filename, re.IGNORECASE)
            year = match.group(1)
            qtr = match.group(2)
            basename = f"{year}QTR{qtr}"

            # --------------------------------------------------------------------------------
            # Process the single listing file
            # --------------------------------------------------------------------------------
            msg = {
                "filepath": filepath,
                "year": year,
                "qtr": qtr,
                "output_csv_directory": output_csv_directory,
                "output_csv_suffix": output_csv_suffix,
                "output_xml_directory": output_xml_directory,
                "basename": basename,
                "num_workers": num_workers,
                "log_level": log_level
            }
            df = director(msg, dispatch, csv_absolute_path_to_save)

            # --------------------------------------------------------------------------------
            # List failed records with 'Filepath' column being None as failed to get XBRL
            # --------------------------------------------------------------------------------
            if any(df['Filepath'].isna()):
                print("*" * 80)
                print("-" * 80)
                print(f"[{len(df['Filepath'].isna())}] failed records:\n")
                print(df[df['Filepath'].isna()])
            else:
                logging.info("main(): all [%s] records processed in %s" % (len(df), filename))
    finally:
        # --------------------------------------------------------------------------------
        # Clean up resource
        # --------------------------------------------------------------------------------
        logging.info("main(): shutting down Ray...")
        ray.shutdown()

