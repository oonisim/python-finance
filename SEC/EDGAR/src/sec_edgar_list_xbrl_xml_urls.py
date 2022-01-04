#!/usr/bin/env python
"""
## Objective
Navigate through the EDGAR XBRL listings to generate the URLs to XBRL XML files.
"""
# ================================================================================
# Setup
# ================================================================================
import logging
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

from sec_edgar_base import (
    EdgarBase
)
from sec_edgar_common import (
    http_get_content,
)
from sec_edgar_constant import (
    DIR_DATA_CSV_INDEX,
    DIR_DATA_CSV_LIST,
    EDGAR_BASE_URL,
    EDGAR_HTTP_HEADERS,
)


class EdgarList(EdgarBase):
    # ================================================================================
    # Init
    # ================================================================================
    def __init__(self):
        super().__init__()

    # ================================================================================
    # Utilities
    # ================================================================================
    @staticmethod
    def input_csv_directory_default():
        return DIR_DATA_CSV_INDEX

    @staticmethod
    def input_csv_suffix_default():
        return ""

    @staticmethod
    def output_csv_directory_default():
        return DIR_DATA_CSV_LIST

    @staticmethod
    def output_csv_suffix_default():
        return "_LIST.gz"

    @staticmethod
    def output_xml_directory_default():
        return "N/A"

    def input_filename_pattern(self):
        """Generate glob pattern to find the input files"""
        pattern = ""
        pattern += f"{self.year}" if self.year else "*"
        pattern += "QTR"
        pattern += f"{self.qtr}" if self.qtr else "?"
        pattern += self.input_csv_suffix if self.input_csv_suffix else ""
        return pattern

    # ================================================================================
    # Logic
    # ================================================================================
    @staticmethod
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

    @staticmethod
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

    @staticmethod
    def find_xbrl_url_from_html_xml(source: bs4.BeautifulSoup, directory: str):
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

    @staticmethod
    def find_xbrl_url_from_xsd(source: bs4.BeautifulSoup, directory: str):
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

    @staticmethod
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

    def xbrl_url(self, index_xml_url: str):
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
            directory = self.filing_directory_path(index_xml_url)

            # --------------------------------------------------------------------------------
            # Look for the XBRL XML file in the index.xml.
            # 1. _htm.xml file
            # 2. <filename>.xml where "filename" is from <filename>.xsd.
            # 3. <filename>.xml where "filename" is not RNN.xml e.g. R10.xml.
            # --------------------------------------------------------------------------------
            source = BeautifulSoup(content, 'html.parser')
            url = self.find_xbrl_url_from_html_xml(source=source, directory=directory)
            if url:
                logging.info(f"XBRL URL identified [%s]" % url)
                return url

            url = self.find_xbrl_url_from_xsd(source=source, directory=directory)
            if url:
                logging.info(f"XBRL URL identified [%s]" % url)
                return url

            url = self.find_xbrl_url_from_auxiliary_regexp(source=source, directory=directory)
            if url:
                logging.info(f"XBRL URL identified [%s]" % url)
                return url

            logging.error("No XBRL identified in the listing [%s]" % index_xml_url)
            return None
        else:
            return None

    @ray.remote(num_returns=1)
    def worker(self, msg):
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
        df.loc[:, 'Filename'] = df['Filename'].apply(lambda txt: self.xbrl_url(self.index_xml_url(txt)))
        return df

    @staticmethod
    def compose_package_to_dispatch_to_worker(msg: dict, task: pd.DataFrame):
        log_level = msg['log_level']
        return {
            "data": task,
            "log_level": log_level
        }

    @staticmethod
    def report_result(msg, result: pd.DataFrame, need_result_data=True) -> str:
        # --------------------------------------------------------------------------------
        # List failed records with 'Filepath' column being None as failed to get XBRL
        # --------------------------------------------------------------------------------
        filename = msg['filename']
        failures = result['Filename'].isna()
        num_failures = failures.values.astype(int).sum()
        num_success = result['Filename'].notna().values.astype(int).sum()

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
    EdgarList().main()
