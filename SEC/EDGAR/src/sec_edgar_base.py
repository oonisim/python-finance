import argparse
import logging
import os
import re

import pandas as pd
import ray

from sec_edgar_common import (
    list_csv_files,
    load_from_csv,
    split,
    output_filepath_for_input_filepath,
    save_to_csv,
)
from sec_edgar_constant import (
    TEST_MODE,
    NUM_CPUS,
    SEC_FORM_TYPE_10K,
    SEC_FORM_TYPE_10Q,
    DEFAULT_LOG_LEVEL,
    DIR_DATA_CSV_LIST,
    DIR_DATA_CSV_XBRL,
    DIR_DATA_XML_XBRL,
)


class EdgarBase:
    @staticmethod
    def input_csv_suffix_default() -> str:
        raise NotImplementedError("TBD")

    @staticmethod
    def output_csv_suffix_default():
        raise NotImplementedError("TBD")

    def get_command_line_arguments(self):
        """Get configurable parameters from the command line"""
        parser = argparse.ArgumentParser(description='EDGAR program')
        parser.add_argument(
            '-ic', '--input-csv-directory', type=str, required=False,
            default=DIR_DATA_CSV_LIST,
            help='specify the input data directory'
        )
        parser.add_argument(
            '-is', '--input-csv-suffix', type=str, required=False,
            default=self.input_csv_suffix_default(),
            help=f'specify the input filename suffix e.g {self.input_csv_suffix_default()}'
        )
        parser.add_argument(
            '-oc', '--output-csv-directory', type=str, required=False,
            default=DIR_DATA_CSV_XBRL,
            help='specify the output data directory to save the csv file (not xml)'
        )
        parser.add_argument(
            '-os', '--output-csv-suffix', type=str, required=False,
            default=self.output_csv_suffix_default(),
            help=f'specify the output csv filename suffix e.g {self.output_csv_suffix_default()}'
        )
        parser.add_argument(
            '-ox', '--output-xml-directory', type=str, required=False,
            default=DIR_DATA_XML_XBRL,
            help='specify the output data directory to save the xml file (not csv)'
        )
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
            '-n', '--num-workers', type=int, required=False,
            default=NUM_CPUS,
            help='specify the number of workers to use'
        )
        parser.add_argument(
            '-l', '--log-level', type=int, choices=[10, 20, 30, 40], required=False,
            default=DEFAULT_LOG_LEVEL,
            help='specify the logging level (10 for INFO)',
        )
        args = vars(parser.parse_args())
        return args

    @staticmethod
    def input_filename_pattern(year: str, qtr: str, suffix: str) -> str:
        raise NotImplementedError("TBD")

    @staticmethod
    def csv_absolute_path_to_save(output_csv_directory, basename, output_csv_suffix):
        raise NotImplementedError("TBD")

    @staticmethod
    @ray.remote(num_returns=1)
    def worker(msg: dict):
        raise NotImplementedError("TBD")

    @staticmethod
    def compose_package_to_dispatch_to_worker(msg: dict, task: pd.DataFrame):
        raise NotImplementedError("TBD")

    def dispatch(self, msg: dict):
        filepath = msg['filepath']
        num_workers = msg['num_workers']

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
            self.worker.remote(self.compose_package_to_dispatch_to_worker(msg, task))
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
        assert isinstance(num_workers, int) and num_workers > 0

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
        self.report_result(msg, df)

        return df

    # --------------------------------------------------------------------------------
    # Main
    # --------------------------------------------------------------------------------
    def main(self):
        # --------------------------------------------------------------------------------
        # Command line arguments
        # --------------------------------------------------------------------------------
        args:dict = self.get_command_line_arguments()

        # Mandatory
        input_csv_directory = args['input_csv_directory']
        input_csv_suffix = args['input_csv_suffix']
        output_csv_directory = args['output_csv_directory']
        output_csv_suffix = args['output_csv_suffix']

        # Optional
        year = str(args['year'])
        qtr = str(args['qtr'])
        num_workers = args['num_workers']
        log_level = args['log_level']

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
                    input_filename_pattern=self.input_filename_pattern(year, qtr, input_csv_suffix),
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
                # Direct the jobs by workers
                # --------------------------------------------------------------------------------
                msg = args.copy()
                msg['filepath'] = filepath
                msg['filename'] = filename
                msg['basename'] = basename
                result = self.director(msg)

        finally:
            # --------------------------------------------------------------------------------
            # Clean up resource
            # --------------------------------------------------------------------------------
            logging.info("main(): shutting down Ray...")
            ray.shutdown()