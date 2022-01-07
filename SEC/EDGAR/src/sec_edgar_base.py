"""
--------------------------------------------------------------------------------
# SEC EDGAR
--------------------------------------------------------------------------------
* [Where Can I Find a Company's Annual Report and Its SEC Filings?]
(https://www.investopedia.com/ask/answers/119.asp)

> If you want to dig deeper and go beyond the slick marketing version of the
annual report found on corporate websites, you'll have to search through
required filings made to the Securities and Exchange Commission.
>
> All publicly-traded companies in the U.S. must file regular financial reports
with the SEC. These filings include the annual report (known as the 10-K),
quarterly report (10-Q), and a myriad of other forms containing all types of
financial data.

# [EDGAR Quarterly Filing Indices]
* [Accessing EDGAR Data](https://www.sec.gov/os/accessing-edgar-data)

> Using the EDGAR index files
Indexes to all public filings are available from 1994Q3 through the present and
located in the following browsable directories:
> * https://www.sec.gov/Archives/edgar/daily-index/
> — daily index files through the current year; (**DO NOT forget the trailing slash '/'**)
> * https://www.sec.gov/Archives/edgar/full-index/
> — full indexes offer a "bridge" between quarterly and daily indexes,
> compiling filings from the beginning of the current quarter through the
> previous business day. At the end of the quarter, the full index is rolled
> into a static quarterly index.
>
> Each directory and all child sub directories contain three files to assist
> in automated crawling of these directories. Note that these are not visible
> through directory browsing.
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

--------------------------------------------------------------------------------
# Master index file for XBRL
--------------------------------------------------------------------------------
<YYYY>/<QTR>/xbrl.gz in https://www.sec.gov/Archives/edgar/full-index" is
the master index file for the filings in XML format at each YYYY/QTR.
+-- <YYYY>
    +-- <QTR>
        +-- xbrl.gz

The master index file is a CSV with the format:
|CIK    |Company Name|Form Type|Filing Date|TXT Path                                   |
|-------|------------|---------|-----------|-------------------------------------------|
|1002047|NetApp, Inc.|10-Q     |2020-02-18 |edgar/data/1002047/0001564590-20-005025.txt|

Each row points to the TXT file which is all-in-one file for the filing where
each document is segmented with <DOCUMENT> tag. However, it is not easy to
parse the TXT to extract each financial statement (FS).

Instead, use the XBRL XML file which each filing director contains.

--------------------------------------------------------------------------------
# EDGAR Directory Listing
--------------------------------------------------------------------------------
Each filing directory has index.xml that lists all the files in the directory.
https://sec.gov/Archives/edgar/full-index/${YEAR}/${QTR}/${ACCESSION}/index.xml

Hence, identify the URL to XBRL XML from index.xml. However, the filename
is not consistent, e.g. <name>_htm.xml, <name>.xml. You need to find it out.

--------------------------------------------------------------------------------
Local Directory Structure and Naming Conventions
--------------------------------------------------------------------------------
${DIR_DATA}
+-- csv
¦   +-- index
¦   ¦   +-- <YYYY>QTR<QTR>            <--- EDGAR master index for the year YYYY and quarter Q
¦   +-- listing
¦   ¦   +-- <YYYY>QTR<QTR>_LIST.gz    <--- SEC filing directory index.xml listings for the year and quarter
¦   +-- xbrl
¦       +-- <YYYY>QTR<QTR>_XBRL.gz    <--- Path list to the SEC filing XBRL XML files the year and quarter
+-- xml
    +-- xbrl
        +-- <CIK>
            +-- <ACCESSION>
                +-- <XBRL>.gz       <--- SEC filing XBRL XML file for the CIK and ACCESSION.

1. Download master index file for XBRL for each YYYY/QTR.
   The result is <YYYY>QTR<QTR>. See the shell script.
2. Iterate through <YYYY>QTR<QTR> index file to identify the URL to XBRL XML.
   The result is <YYYY>QTR<QTR>_LIST.gz.
3. Iterate through <YYYY>QTR<QTR>_LIST.gz to download XBRL XML for CIK/ACCESSION
   and record the local file path to the files.
   The result is <YYYY>QTR<QTR>_XBRL.gz and an XML file in <CIK/<ACCESSION>/.
"""
import argparse
import logging
import os
import pathlib
import re
from typing import (
    Callable
)

import pandas as pd
import ray
import time

from sec_edgar_common import (
    list_csv_files,
    load_from_csv,
    split,
    save_to_csv,
)
from sec_edgar_constant import (
    NUM_CPUS,
    MAX_NUM_WORKERS,
    SEC_FORM_TYPE_10K,
    SEC_FORM_TYPE_10Q,
    DEFAULT_LOG_LEVEL,
)


class EdgarBase:
    # ================================================================================
    # Init
    # ================================================================================
    def __init__(self):
        # --------------------------------------------------------------------------------
        # Parameters
        # --------------------------------------------------------------------------------
        args = self.get_command_line_arguments()
        self.args = args

        # SEC Filing filter
        self.input_csv_directory = args['input_csv_directory']
        self.input_csv_suffix = args['input_csv_suffix']
        self.output_csv_directory = args['output_csv_directory']
        self.output_csv_suffix = args['output_csv_suffix']
        self.output_xml_directory = args['output_xml_directory']
        # Filing filter
        self.year = str(args['year']) if args['year'] else None
        self.qtr = str(args['qtr']) if args['qtr'] else None
        self.form_types = str(args['form_types'])
        # Platform configurations
        self.num_workers = args['num_workers']
        self.log_level = args['log_level']
        self.test_mode = args["test_mode"]

        # State of the output CSV directory creation
        self.output_csv_directory_created = False

        # --------------------------------------------------------------------------------
        # Logging
        # --------------------------------------------------------------------------------
        logging.basicConfig(level=args["test_mode"])

        # --------------------------------------------------------------------------------
        # Pandas
        # --------------------------------------------------------------------------------
        pd.set_option('display.float_format', lambda x: ('%f' % x).rstrip('0').rstrip('.'))
        pd.set_option('display.colheader_justify', 'center')
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)

    # ================================================================================
    # Utility
    # ================================================================================
    @staticmethod
    def input_csv_directory_default():
        raise NotImplementedError("TBD")

    @staticmethod
    def input_csv_suffix_default() -> str:
        raise NotImplementedError("TBD")

    @staticmethod
    def input_xml_directory_default():
        return None

    @staticmethod
    def output_csv_directory_default():
        raise NotImplementedError("TBD")

    @staticmethod
    def output_csv_suffix_default():
        raise NotImplementedError("TBD")

    @staticmethod
    def output_xml_directory_default():
        raise NotImplementedError("TBD")

    @staticmethod
    def output_xml_suffix_default():
        raise NotImplementedError("TBD")

    def get_command_line_arguments(self):
        """Get configurable parameters from the command line"""
        parser = argparse.ArgumentParser(description='EDGAR program')

        # --------------------------------------------------------------------------------
        # Data sources
        # --------------------------------------------------------------------------------
        parser.add_argument(
            '-ic', '--input-csv-directory', type=str, required=False,
            default=self.input_csv_directory_default(),
            help='specify the input CSV directory'
        )
        parser.add_argument(
            '-ics', '--input-csv-suffix', type=str, required=False,
            default=self.input_csv_suffix_default(),
            help=f'specify the input csv filename suffix e.g {self.input_csv_suffix_default()}'
        )
        parser.add_argument(
            '-ix', '--input-xml-directory', type=str, required=False,
            default=self.input_xml_directory_default(),
            help='specify the input XML directory'
        )
        parser.add_argument(
            '-oc', '--output-csv-directory', type=str, required=False,
            default=self.output_csv_directory_default(),
            help='specify the output data directory to save the csv file (not xml)'
        )
        parser.add_argument(
            '-os', '--output-csv-suffix', type=str, required=False,
            default=self.output_csv_suffix_default(),
            help=f'specify the output csv filename suffix e.g {self.output_csv_suffix_default()}'
        )
        parser.add_argument(
            '-ox', '--output-xml-directory', type=str, required=False,
            default=self.output_xml_directory_default(),
            help='specify the output data directory to save the xml file (not csv)'
        )
        # --------------------------------------------------------------------------------
        # SEC Filing filter
        # --------------------------------------------------------------------------------
        parser.add_argument(
            '-y', '--year', type=int, required=False,
            default=None,
            help='specify the year of the filing'
        )
        parser.add_argument(
            '-q', '--qtr', type=int, choices=[1, 2, 3, 4], required=False,
            default=None,
            help='specify the quarter of the filing'
        )
        parser.add_argument(
            '-f', '--form-types', type=str.upper, nargs='+', required=False,
            default=[SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K],
            help='specify the form types to select e.g. 10-Q, 10-K'
        )

        # --------------------------------------------------------------------------------
        # Platform configurations
        # --------------------------------------------------------------------------------
        parser.add_argument(
            '-n', '--num-workers', type=int, required=False,
            default=NUM_CPUS,
            help='specify the number of workers to use'
        )
        parser.add_argument(
            '-l', '--log-level', type=int,
            choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR],
            required=False,
            default=DEFAULT_LOG_LEVEL,
            help='specify the logging level (20 for INFO)',
        )
        parser.add_argument(
            '-t', '--test-mode', action="store_true",
            help='specify to enable the test mode'
        )

        # --------------------------------------------------------------------------------
        # Validations
        # --------------------------------------------------------------------------------
        args = vars(parser.parse_args())

        # Directories
        args['input_csv_directory'] = os.path.realpath(args['input_csv_directory'])
        args['input_xml_directory'] = os.path.realpath(args['input_xml_directory']) \
            if args['input_xml_directory'] else None
        args['output_csv_directory'] = os.path.realpath(args['output_csv_directory'])
        args['output_xml_directory'] = os.path.realpath(args['output_xml_directory']) \
            if args['output_xml_directory'] else None
        assert os.path.isdir(args['input_csv_directory'])
        # Valid output directory (cannot check until the directory is created)
        # assert os.path.isdir(args['output_csv_directory'])
        # Valid output directory (cannot check until the directory is created)
        # assert os.path.isdir(args['output_csv_directory'])

        # Form types
        assert all([
            form in [SEC_FORM_TYPE_10Q, SEC_FORM_TYPE_10K]
            for form in args['form_types']
        ]), "Invalid form type(s) in [%s]" % args['form_types']

        # Number of workers
        assert 0 < args['num_workers'] < MAX_NUM_WORKERS, \
            f"Invalid number of workers {args['num_workers']}"

        # Test mode
        assert isinstance(args["test_mode"], bool), \
            f"Invalid test_mode {type(args['test_mode'])}"

        return args

    def input_filename_pattern(self) -> str:
        """Generate glob pattern to find the input files"""
        pattern = ""
        pattern += f"{self.year}" if self.year else "*"
        pattern += "QTR"
        pattern += f"{self.qtr}" if self.qtr else "?"
        pattern += self.input_csv_suffix if self.input_csv_suffix is not None else ""
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

    def csv_absolute_path_to_save(self, directory: str, basename: str, suffix: str):
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

    # ================================================================================
    # Processing
    # ================================================================================
    @ray.remote(num_returns=1)
    def worker(self, msg: dict) -> pd.DataFrame:
        """Worker task to execute based on the instruction message
        Args:
            msg: task instruction message
        """
        raise NotImplementedError("TBD")

    @staticmethod
    def compose_package_to_dispatch_to_worker(msg: dict, task: pd.DataFrame):
        raise NotImplementedError("TBD")

    def dispatch(self, msg: dict):
        filepath = msg['filepath']
        num_workers = msg['num_workers']
        form_types = msg['form_types']
        test_mode = msg['test_mode']

        # --------------------------------------------------------------------------------
        # Load the listing CSV ({YEAR}QTR{QTR}_LIST.gz) into datafame
        # --------------------------------------------------------------------------------
        df = load_from_csv(filepath=filepath, types=form_types)
        assert df is not None and len(df) > 0, "worker(): invalid dataframe"

        # --------------------------------------------------------------------------------
        # Test debug configurations
        # - Limit 3 records per worker.
        # --------------------------------------------------------------------------------
        df = df.head(3 * num_workers) if test_mode else df

        # --------------------------------------------------------------------------------
        # Asynchronously invoke tasks
        # NOTE: Need to pass "self" as worker.remote(self, msg) not worker.remote(msg).
        # Python runtime automatically insert self if it is an instance method, but
        # Ray "remote" proxy is a function, not class instance method.
        # Alternatively make the remote method as static, however you cannot access
        # instance/class members.
        # --------------------------------------------------------------------------------
        futures = [
            self.worker.remote(self, self.compose_package_to_dispatch_to_worker(msg, task))
            for task in split(tasks=df, num=num_workers)
        ]
        assert len(futures) == num_workers, f"Expected {num_workers} tasks but got {len(futures)}."
        return futures

    @staticmethod
    def report_result(msg, result: pd.DataFrame, need_result_data=False) -> str:
        """Generate a report about the job result done by workers"""
        raise NotImplementedError("TBD")

    def director(self, msg: dict):
        """Director to dispatch jobs
        Args:
            msg: message to handle
        Returns: Pandas dataframe of failed records
        """
        basename = msg['basename']
        output_csv_directory = msg["output_csv_directory"]
        output_csv_suffix = msg["output_csv_suffix"]
        num_workers = msg['num_workers']

        # --------------------------------------------------------------------------------
        # Dispatch jobs
        # --------------------------------------------------------------------------------
        futures = self.dispatch(msg)

        # --------------------------------------------------------------------------------
        # Wait for the job completion
        # --------------------------------------------------------------------------------
        waits = []
        while futures:
            completed, futures = ray.wait(futures)
            waits.extend(completed)

        # --------------------------------------------------------------------------------
        # Collect the results
        # --------------------------------------------------------------------------------
        assert len(waits) == num_workers, f"Expected {num_workers} tasks but got {len(waits)}"
        df = pd.concat(ray.get(waits))
        df.sort_index(inplace=True)

        # --------------------------------------------------------------------------------
        # Save the result dataframe
        # --------------------------------------------------------------------------------
        package = {
            "data": df,
            "destination": self.csv_absolute_path_to_save(output_csv_directory, basename, output_csv_suffix),
        }
        save_to_csv(package)

        # --------------------------------------------------------------------------------
        # Report the result
        # --------------------------------------------------------------------------------
        print(self.report_result(msg, df))

        return df

    # ================================================================================
    # Main
    # ================================================================================
    def main(self):
        # --------------------------------------------------------------------------------
        # XBRL XML logic
        # --------------------------------------------------------------------------------
        try:
            # --------------------------------------------------------------------------------
            # Setup Ray
            # --------------------------------------------------------------------------------
            logging.info("main(): initializing Ray using %s workers..." % self.num_workers)
            ray.init(num_cpus=self.num_workers, num_gpus=0, logging_level=self.log_level)

            # --------------------------------------------------------------------------------
            # Process input files
            # --------------------------------------------------------------------------------
            for filepath in list_csv_files(
                    input_csv_directory=self.input_csv_directory,
                    input_filename_pattern=self.input_filename_pattern(),
                    f_output_filepath_for_input_filepath=self.f_csv_absolute_path_to_save_for_input_filepath(
                        self.output_csv_directory, self.output_csv_suffix, self.input_csv_suffix
                    )
            ):
                filename = os.path.basename(filepath)
                logging.info("main(): processing the input csv [%s]..." % filename)

                # --------------------------------------------------------------------------------
                # Year/Quarter of the filing from the filename
                # --------------------------------------------------------------------------------
                match = re.search("^([1-2][0-9]{3})QTR([1-4]).*$", filename, re.IGNORECASE)
                assert len(match.groups()) == len(["year", "qtr"]), \
                    "Insufficient matches: expected %s got %s in %s" % \
                    (len(["year", "qtr"]), len(match.groups()), filename)
                year_from_filename = match.group(1)
                qtr_from_filename = match.group(2)
                basename = f"{year_from_filename}QTR{qtr_from_filename}"

                # --------------------------------------------------------------------------------
                # Execute
                # --------------------------------------------------------------------------------
                msg = self.args.copy()

                # NOTE: year/qtr in the command line argument is to filter the input files only.
                # Hence, overwrite them with the year/qtr of the filing.
                msg['year'] = year_from_filename
                msg['qtr'] = qtr_from_filename
                msg['filepath'] = filepath
                msg['filename'] = filename
                msg['basename'] = basename
                result = self.director(msg)

        finally:
            # --------------------------------------------------------------------------------
            # Clean up resource
            # --------------------------------------------------------------------------------
            logging.info("main(): shutting down Ray...")
            time.sleep(3)
            ray.shutdown()
