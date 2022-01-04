import glob
import logging
import os
import re

import pandas as pd
import requests

# regexp to extract numeric string
REGEXP_NUMERIC = re.compile(r"\s*[\d.-]*\s*")


def filename_basename(filename):
    return os.path.splitext(os.path.basename(filename))[0]


def filename_extension(filename):
    return os.path.splitext(filename)[1]


def split(tasks: pd.DataFrame, num: int):
    """Split tasks into num assignments and dispense them sequentially
    Args:
        tasks: tasks to split into assignments
        num: number of assignments to create
    Yields: An assignment, which is a slice of the tasks
    """
    assert num > 0
    assert len(tasks) > 0
    logging.debug(f"createing {num} assignments for {len(tasks)} tasks")

    # Total size of the tasks
    total = len(tasks)

    # Each assignment has 'quota' size which can be zero if total < number of assignments.
    quota = int(total / num)

    # Left over after each assignment takes its 'quota'
    redisual = total % num

    start = 0
    while start < total:
        # As long as redisual is there, each assginemt has (quota + 1) as its tasks.
        if redisual > 0:
            size = quota + 1
            redisual -= 1
        else:
            size = quota

        end = start + size
        yield tasks[start : min(end, total)]

        start = end
        end += size


def http_get_content(url, headers):
    """HTTP GET URL content
    Args:
        url: URL to GET
    Returns:
        Content of the HTTP GET response body, or None
    Raises:
        ConnectionError if HTTP status is not 200
    """
    logging.debug("http_get_content(): GET url [%s] headers [%s]" % (url, headers))

    error = None
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        if response.status_code == 200:
            content = response.content.decode("utf-8")
            return content
    except requests.exceptions.HTTPError as e:
        error = e
        logging.error("http_get_content(): HTTP error %s" % e)
    except requests.exceptions.ConnectionError as e:
        error = e
        logging.error("http_get_content(): HTTP error %s" % e)
    except requests.exceptions.Timeout as e:
        error = e
        logging.error("http_get_content(): HTTP error %s" % e)
    except requests.exceptions.RequestException as e:
        error = e
        logging.error("http_get_content(): HTTP error %s" % e)

    assert error is not None
    raise RuntimeError("HTTP to SEC EDGAR failed") from error


def list_csv_files(
        input_csv_directory,
        input_filename_pattern,
        f_output_filepath_for_input_filepath
    ):
    """List files in the directory that are to be processed.
    When year is specified, only matching listing files for the year will be selected.
    When qtr is specified, only matching listing files for the quarter will be selected.

    Args:
        input_csv_directory: path to the directory from where to get the file
        input_filename_pattern: glob pattern to list the input files.
        f_output_filepath_for_input_filepath: function to generate the output filepath.

    Returns: List of flies to purocess
    """
    assert os.path.isdir(input_csv_directory), f"Not a directory or does not exist: {input_csv_directory}"

    def is_file_to_process(filepath):
        """Verify if the filepath points to a file that has not been processed yet.
        If output file has been already created and exists, then skip the filepath.
        """
        if os.path.isfile(filepath) and os.access(filepath, os.R_OK):
            # --------------------------------------------------------------------------------
            # Check if the output file to generate already exists (has been created).
            # The program can be run multiple times, hence avoid processing the file
            # that has been already processed with the output file created.
            # --------------------------------------------------------------------------------
            output_filepath = f_output_filepath_for_input_filepath(filepath)
            if os.path.isfile(output_filepath):
                logging.info(
                    "list_csv_files(): skip input file [%s] as output file [%s] already exits."
                    % (filepath, output_filepath)
                )
                return False
            else:
                logging.info("list_csv_files(): adding [%s] to handle" % filepath)
                return True
        else:
            logging.error("[%s] does not exist, cannot read, or not a file. skipping." % filepath)
            return False

    logging.info("Listing the files to process in the directory %s ..." % input_csv_directory)
    files = sorted(
        filter(is_file_to_process, glob.glob(input_csv_directory + os.sep + input_filename_pattern)),
        reverse=True
    )
    if files is None or len(files) == 0:
        logging.info("No files to process in the directory %s" % input_csv_directory)

    return files


def load_from_csv(filepath, types):
    """Create a dataframe from a csv that has the format:
    |CIK|Company Name|Form Type|Date Filed|Filename|

    Args:
        filepath: path to the csv file
        types: Filing types e.g. 10-K
    Returns:
        pandas dataframe
    """
    logging.info("load_from_csv(): filepath [%s]" % filepath)
    assert os.path.isfile(filepath) and os.access(filepath, os.R_OK), \
        f"{filepath} is not a file, cannot read, or does not exist."

    # --------------------------------------------------------------------------------
    # Load XBRL index CSV file
    # --------------------------------------------------------------------------------
    df = pd.read_csv(
        filepath,
        skip_blank_lines=True,
        header=0,         # The 1st data line after omitting skiprows and blank lines.
        sep='|',
    )

    # --------------------------------------------------------------------------------
    # Select filing for the target Form Types e.g. 10-K
    # --------------------------------------------------------------------------------
    df = df.loc[df['Form Type'].isin(types)] if types else df
    df.loc[:, 'Form Type'] = df['Form Type'].astype('category')
    logging.info("load_from_csv(): size of df [%s]" % len(df))

    return df


def save_to_csv(msg):
    """Save the dataframe to CSV"""
    df = msg['data']                            # Dataframe to save
    destination: str = msg['destination']       # Absolute path to save csv

    logging.info("save_to_csv(): saving dataframe to [%s]..." % destination)
    try:
        df.to_csv(
            destination,
            sep="|",
            header=True,
            index=False
        )
    except IOError as e:
        logging.error("save_to_csv(): failed to save [%s] due to [%s]" % (destination, e))
        raise RuntimeError("save_to_csv(): failed") from e
