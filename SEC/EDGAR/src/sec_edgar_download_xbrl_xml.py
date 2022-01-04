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
import gzip
import logging
# ================================================================================
# Setup
# ================================================================================
import os
import pathlib
import random
import re

import pandas as pd
import ray
import time

from sec_edgar_base import (
    EdgarBase
)
from sec_edgar_common import (
    filename_extension,
    http_get_content,
)
from sec_edgar_constant import (
    EDGAR_HTTP_HEADERS,
)

pd.set_option('display.max_colwidth', None)


# ================================================================================
# Utilities
# ================================================================================
class EdgarXBRL(EdgarBase):
    @staticmethod
    def input_csv_suffix_default():
        return "_LIST.gz"

    @staticmethod
    def output_csv_suffix_default():
        return "_XBRL.gz"

    @staticmethod
    def xml_relative_path_to_save(directory: str, basename: str):
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

    def xml_absolute_path_to_save(self, output_xml_directory: str, directory: str, basename: str):
        """
        Generate the absolute file path to save the XBRL XML. Create directory if required.
        Each XML is saved to {output_xml_directory}/{directory}/{basename}.gz.

        Args:
            output_xml_directory: Base directory for XML
            directory: Relative path from the output_xml_directory
            basename: basename of the file to save
        Returns: Absolute file path
        """
        relative = self.xml_relative_path_to_save(directory, basename)
        absolute = os.path.realpath(f"{output_xml_directory}{os.sep}{relative}")
        pathlib.Path(os.path.dirname(absolute)).mkdir(mode=0o775, parents=True, exist_ok=True)
        return absolute

    def csv_absolute_path_to_save(self, directory: str, basename: str, suffix: str=""):
        """
        Generate the absolute file path to save the dataframe as csv. Create directory if required.

        Args:
            directory: *Absolute* path to the directory to save the file
            basename: Name of the file to save
            suffix: Suffix of the file

        Returns: Absolute file path
        """
        if not hasattr(self.csv_absolute_path_to_save, "mkdired"):
            pathlib.Path(directory).mkdir(mode=0o775, parents=True, exist_ok=True)
            setattr(self.csv_absolute_path_to_save, "mkdired", True)

        absolute = os.path.realpath(f"{directory}{os.sep}{basename}{suffix}")
        return absolute

    @staticmethod
    def input_filename_pattern(year: str, qtr: str, suffix: str):
        """Generate glob pattern to find the input files"""
        pattern = ""
        pattern += f"{year}" if year else "*"
        pattern += "QTR"
        pattern += f"{qtr}" if qtr else "?"
        pattern += suffix if suffix is not None else ""
        return pattern

    @staticmethod
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

    def save_to_xml(self, msg: dict):
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

        destination = self.xml_absolute_path_to_save(
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
            return self.xml_relative_path_to_save(directory, basename)

    # ================================================================================
    # Logic
    # ================================================================================
    @ray.remote(num_returns=1)
    def worker(self, msg: dict) -> pd.DataFrame:
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
            content = self.get_xml(url)

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
                path_to_saved_xml = self.save_to_xml(package)

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

    @staticmethod
    def compose_package_to_dispatch_to_worker(msg: dict, task: pd.DataFrame):
        year = msg['year']
        qtr = msg['qtr']
        output_xml_directory = msg["output_xml_directory"]
        log_level = msg['log_level']

        return {
            "data": task,
            "year": year,
            "qtr": qtr,
            "output_xml_directory": output_xml_directory,
            "log_level": log_level
        }

    @staticmethod
    def report_result(msg, result: pd.DataFrame, need_result_data=True) -> str:
        # --------------------------------------------------------------------------------
        # List failed records with 'Filepath' column being None as failed to get XBRL
        # --------------------------------------------------------------------------------
        filename = msg['filename']
        failures = result['Filepath'].isna()
        num_failures = failures.values.astype(int).sum()
        num_success = result['Filepath'].notna().values.astype(int).sum()

        record_counts = f"Processed [{num_success}] with [{num_failures}] failed in [{filename}]."
        if num_failures > 0 and need_result_data:
            report = "{sepline}\n{record_counts}\n{records}".format(
                sepline="*" * 80,
                record_counts=record_counts,
                records=result[failures]
            )
        else:
            report = record_counts

        return report


# --------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    EdgarXBRL().main()
