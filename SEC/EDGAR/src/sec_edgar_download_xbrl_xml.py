#!/usr/bin/env python
"""Navigate through the EDGAR XBRL listings to generate the URLs to XBRL XML files.
"""
import gzip
import logging
import os
import pathlib
import random
import re
# ================================================================================
# Setup
# ================================================================================
from typing import (
    Callable
)

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


class EdgarXBRL(EdgarBase):
    # ================================================================================
    # Init
    # ================================================================================
    def __init__(self):
        super().__init__()

        self.output_csv_directory_created = False

    # ================================================================================
    # Utilities
    # ================================================================================
    @staticmethod
    def input_csv_suffix_default():
        return "_LIST.gz"

    @staticmethod
    def output_csv_suffix_default():
        return "_XBRL.gz"

    def input_filename_pattern(self):
        """Generate glob pattern to find the input files"""
        pattern = ""
        pattern += f"{self.year}" if self.year else "*"
        pattern += "QTR"
        pattern += f"{self.qtr}" if self.qtr else "?"
        pattern += self.input_csv_suffix if self.input_csv_suffix else ""
        return pattern

    @staticmethod
    def f_csv_absolute_path_to_save_for_input_filepath(
            output_csv_directory, output_csv_suffix, input_csv_suffix
    ) -> Callable:
        """
        Generate the function to provide the path to the output file
        to be created for the input path.

        csv_absolute_path_to_save() does not know the suffix of input file,
        hence instead of using csv_absolute_path_to_save(), use this one.

        Args:
            output_csv_directory: Directory to save the output CSV files
            output_csv_suffix: Suffix of the output CSV file e.g. _XBRL.gz
            input_csv_suffix: Suffix of the input CSV file e.g. _LIST.gz
        Returns: Function to generate the output filepath
        """
        def f(input_filepath):
            """
            Generate the absolute path to the output CSV to save for the input CSV filepath
            Args:
                input_filepath: Absolute path to the input csv file
            Returns: Absolute path to the output CSV file to save
            """
            filename = os.path.basename(input_filepath)
            assert filename.endswith(input_csv_suffix), \
                "input filepath {} must ends with suffix {}".format(filename, input_csv_suffix)

            base = filename.rstrip(input_csv_suffix)
            assert len(base) > 0, "base [{}] must have {{YYYY}}QTR{{Q}} format".format(base)

            return f"{output_csv_directory}{os.sep}{base}{output_csv_suffix}"

        return f

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
        if not self.output_csv_directory_created:
            try:
                pathlib.Path(directory).mkdir(mode=0o775, parents=True, exist_ok=True)
                self.output_csv_directory_created = True
            except OSError as e:
                logging.error(f"csv_absolute_path_to_save(): mkdir [%s] failed due to [%s]" % (directory, e))
                raise RuntimeError("csv_absolute_path_to_save() failed.") from e

        absolute = os.path.realpath(f"{directory}{os.sep}{basename}{suffix}")
        return absolute

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
        [NOTE]: Need to pass "self" as worker.remote(self, msg) not worker.remote(msg).
        Python runtime automatically insert self if it is an instance method, but
        Ray "remote" proxy is a function, not class instance method.
        Alternatively make the remote method as static, however you cannot access
        instance/class members.

        1. GET XBRL XML from the filing directory and save to a file.
        2. Update the listing dataframe with the year, qtr, path to the saved XBRL XML.
           Set None to the path column when failed to get the XBRL XML.

        The incoming dataframe has the format:
        |CIK|Company Name|Form Type|Date Filed|Filename|

        'Filename' is the URL to XBRL XML in the filing directory with the format:
        https://sec.gov/Archives/edgar/data/{cik}}/{accession}}/{filename}
        https://sec.gov/Archives/edgar/data/1000697/000095012310017583/wat-20091231.xml

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
        # Insert Year, Quarter columns before "Filename", and "Filepath" at the end
        # --------------------------------------------------------------------------------
        assert year.isdecimal() and re.match(r"^[12][0-9]{3}$", year)
        assert qtr.isdecimal() and re.match(r"^[1-4]$", qtr)
        df.insert(loc=df.columns.get_loc("Filename"), column='Year', value=pd.Categorical([year]*len(df)))
        df.insert(loc=df.columns.get_loc("Year")+1, column='Quarter', value=pd.Categorical([qtr]*len(df)))
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
                    "CIK [%s] must match CIK part [%s] in url [%s] in the row [%s]" % \
                    (row['CIK'], cik, url, row)

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
                    logging.error(
                        "worker(): not updated the dataframe[%s, 'Filepath'] as saving XML has failed."
                        % index
                    )
            else:
                logging.error(
                    "worker(): not updated the dataframe[%s, 'Filepath'] as failed to get XBRL XML."
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
