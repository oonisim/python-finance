import gzip
import logging
import os
import re
from typing import (
    List,
    Callable,
    Optional
)

import bs4
import itertools
import pandas as pd
import ray
from bs4 import BeautifulSoup

from sec_edgar_base import (
    EdgarBase
)
from sec_edgar_common import (
    filename_extension,
)
from sec_edgar_constant import (
    # DIR
    DIR_DATA_CSV_XBRL,
    DIR_DATA_XML_XBRL,
    DIR_DATA_CSV_GAAP,
    # DF
    DF_COLUMN_CIK,
    DF_COLUMN_COMPANY,
    DF_COLUMN_FORM_TYPE,
    DF_COLUMN_DATE_FILED,
    DF_COLUMN_FILENAME,
    DF_COLUMN_FILEPATH,
    DF_COLUMN_ACCESSION,
    DF_COLUMN_FS,
    DF_COLUMN_YEAR,
    DF_COLUMN_QTR,
    DF_COLUMN_CONTEXT,
)
from xbrl_gaap_function import (
    get_company_name,
    get_date_from_xbrl_filename,
    get_attributes_to_select_target_fs_elements,
    get_financial_element_columns,
    # sed -n 's/^def \(get_pl_.*\)(.*/\1,/p'
    get_pl_revenues,
    get_pl_cost_of_revenues,
    get_pl_gross_profit,
    get_pl_operating_expense_r_and_d,
    get_pl_operating_expense_selling_administrative,
    get_pl_operating_expense_other,
    get_pl_operating_expense_total,
    get_pl_operating_income,
    get_pl_non_operating_expense_interest,
    get_pl_non_operating_expense_other,
    get_pl_income_tax,
    get_pl_net_income,
    get_pl_shares_outstanding,
    get_pl_eps,
    # sed -n 's/^def \(get_bs_.*\)(.*/\1,/p'
    get_bs_current_asset_cash_and_equivalents,
    get_bs_current_asset_restricted_cash_and_equivalents,
    get_bs_current_asset_short_term_investments,
    get_bs_current_asset_account_receivables,
    get_bs_current_asset_inventory,
    get_bs_current_asset_other,
    get_bs_current_assets,
    get_bs_non_current_asset_property_and_equipment,
    get_bs_non_current_asset_restricted_cash_and_equivalent,
    get_bs_non_current_asset_deferred_income_tax,
    get_bs_non_current_asset_goodwill,
    get_bs_non_current_asset_other,
    get_bs_total_assets,
    get_bs_current_liability_account_payable,
    get_bs_current_liability_tax,
    get_bs_current_liability_longterm_debt,
    get_bs_current_liabilities,
    get_bs_non_current_liability_longterm_debt,
    get_bs_non_current_liability_deferred_tax,
    get_bs_non_current_liability_other,
    get_bs_total_liabilities,
    get_bs_stockholders_equity_paid_in,
    get_bs_stockholders_equity_retained,
    get_bs_stockholders_equity_other,
    get_bs_stockholders_equity,
    get_bs_total_liabilities_and_stockholders_equity,
)

PL_FUNCTIONS: List[Callable] = [
    get_pl_revenues,
    get_pl_cost_of_revenues,
    get_pl_gross_profit,
    get_pl_operating_expense_r_and_d,
    get_pl_operating_expense_selling_administrative,
    get_pl_operating_expense_other,
    get_pl_operating_expense_total,
    get_pl_operating_income,
    get_pl_non_operating_expense_interest,
    get_pl_non_operating_expense_other,
    get_pl_income_tax,
    get_pl_net_income,
    get_pl_shares_outstanding,
    get_pl_eps,
]

BS_FUNCTIONS: List[Callable] = [
    get_bs_current_asset_cash_and_equivalents,
    get_bs_current_asset_restricted_cash_and_equivalents,
    get_bs_current_asset_short_term_investments,
    get_bs_current_asset_account_receivables,
    get_bs_current_asset_inventory,
    get_bs_current_asset_other,
    get_bs_current_assets,
    get_bs_non_current_asset_property_and_equipment,
    get_bs_non_current_asset_restricted_cash_and_equivalent,
    get_bs_non_current_asset_deferred_income_tax,
    get_bs_non_current_asset_goodwill,
    get_bs_non_current_asset_other,
    get_bs_total_assets,
    get_bs_current_liability_account_payable,
    get_bs_current_liability_tax,
    get_bs_current_liability_longterm_debt,
    get_bs_current_liabilities,
    get_bs_non_current_liability_longterm_debt,
    get_bs_non_current_liability_deferred_tax,
    get_bs_non_current_liability_other,
    get_bs_total_liabilities,
    get_bs_stockholders_equity_paid_in,
    get_bs_stockholders_equity_retained,
    get_bs_stockholders_equity_other,
    get_bs_stockholders_equity,
    get_bs_total_liabilities_and_stockholders_equity,
]


class EdgarGAAP(EdgarBase):
    # ================================================================================
    # Init
    # ================================================================================
    def __init__(self):
        super().__init__()

    # ================================================================================
    # Logic
    # ================================================================================
    @staticmethod
    def input_csv_directory_default():
        return DIR_DATA_CSV_XBRL

    @staticmethod
    def input_csv_suffix_default():
        return "_XBRL.gz"

    @staticmethod
    def input_xml_directory_default():
        return DIR_DATA_XML_XBRL

    @staticmethod
    def output_csv_directory_default():
        return DIR_DATA_CSV_GAAP

    @staticmethod
    def output_csv_suffix_default():
        return "_GAAP.gz"

    @staticmethod
    def output_xml_directory_default():
        return "N/A"

    @staticmethod
    def output_xml_suffix_default():
        return "N/A"

    @staticmethod
    def validate_year_qtr(row, year, qtr):
        assert str(year) == str(row[DF_COLUMN_YEAR]), \
            "Year mismatch. msg['year'] is [%s] but row['year'] is [%s]." % \
            (year, row[DF_COLUMN_YEAR])
        assert str(qtr) == str(row[DF_COLUMN_QTR]), \
            "Year mismatch. msg['qtr'] is [%s] but row['qtr'] is [%s]." % \
            (qtr, row[DF_COLUMN_QTR])

    @staticmethod
    def load_from_xml(filepath: str) -> str:
        """Load the XML contents from the filepath
        Args:
            filepath: path to the XML file
        Returns: XML content
        """
        logging.debug("load_from_xml(): loading XML from [%s]" % filepath)
        try:
            extension = filename_extension(filepath)
            if extension == ".gz" or extension == ".gip":
                with gzip.open(filepath, "rb") as f:
                    bytes = f.read()
                    content = bytes.decode("utf-8")
            else:
                with open(filepath, "r") as f:
                    content = f.read()

        except OSError as e:
            logging.error(
                "load_from_xml():failed to read [%s] for [%s]." % (filepath, e)
            )
            raise RuntimeError("load_from_xml()") from e
        return content

    @staticmethod
    def load_xbrl(filepath: str):
        """Load the XBRL XML as BS4
        Using HTML parser because:

        BS4/XML Parser requires namespace definitions (xmlns=...) to be able to
        handle namespaced tags e.g. (us-gaap:Revenue) and it is case-sensitive.
        In case the XBRL XML does not provide the namespaces, or there are
        mistakes in case in tag names, it will break.

        BS4/HTML parser converts cases to lower, hence be able to handle tags
        in case in-sensitive manner. Namespaced tags e.g. us-gaap:revenue
        is regarded as a single tag.
        """
        xml = EdgarGAAP.load_from_xml(filepath)
        source = BeautifulSoup(xml, 'html.parser')
        del xml
        return source

    @staticmethod
    def get_accession_from_xbrl_filepath(filepath):
        """Get the filing accession id
        The XBRL XML file path has the format
        ${DIR_DATA_CSV_GAAP}/{CIK}/{ACCESSION}/<filename>.xml.gz
        """
        accession = filepath.split(os.sep)[-2]
        logging.debug("get_accession_from_xbrl_filepath(): accession is [%s]" % accession)
        return accession

    @staticmethod
    def get_PL(xbrl: bs4.BeautifulSoup, attributes: dict) -> List[List[str]]:
        """Generate PL (Income Statement) records.
        Args:
            xbrl: XBRL XML datasource
            attributes: XML attributes to match the XML elements
        Returns:
            List of FS records with the format:
            |FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        return list(itertools.chain(*[f(xbrl, attributes) for f in PL_FUNCTIONS]))

    @staticmethod
    def get_BS(xbrl: bs4.BeautifulSoup, attributes: dict) -> List[List[str]]:
        """Generate BS records.
        Args:
            xbrl: XBRL XML datasource
            attributes: XML attributes to match the XML elements
        Returns:
            List of FS records with the format:
            |FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        # sum() is slow -> https://stackoverflow.com/a/952946
        # return sum([f(xbrl, attributes) for f in BS_FUNCTIONS], [])
        return list(itertools.chain(*[f(xbrl, attributes) for f in BS_FUNCTIONS]))

    @staticmethod
    def prepend_columns(
            fs: List[List[str]], columns: List[str]
    ) -> List[List[str]]:
        """Prepend the columns to annotate each record e.g. with CIK, Form Type, etc.
        """
        for row in fs:
            # Add reverse order e.g. if columns are CIK, Accession, ...:
            # 1. add accession at 0
            # 2. add cik at 0
            # Then it will be cik|accession|...
            for col in columns[::-1]:
                row.insert(0, col)

        logging.debug("prepend_columns(): First row of FS:\n[%s]" % fs[0])
        return fs

    @staticmethod
    def get_financial_statements(msg: dict, row) -> Optional[List[List[str]]]:
        """Generate a list of financial statement elements from a filing
        Args:
            msg: message
            row: |CIK|Form Type|Year|Quarter|Filepath|
        Returns:
            List of FS records with the format:
            |CIK|Accession|Form Type|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        filepath = row[DF_COLUMN_FILEPATH]
        cik = row[DF_COLUMN_CIK]
        year = row[DF_COLUMN_YEAR]
        qtr = row[DF_COLUMN_QTR]
        if not (isinstance(filepath, str) and len(filepath) > 0):
            logging.error(
                "get_financial_statements(): skipping CIK[%s] Year[%s] Qtr[%s] "
                "as no Filepath in row:\n[%s]" % (cik, year, qtr, row)
            )
            return None

        form_type = row[DF_COLUMN_FORM_TYPE]
        input_xml_directory = msg['input_xml_directory']
        accession = EdgarGAAP.get_accession_from_xbrl_filepath(filepath)
        logging.info(
            "get_financial_statements(): processing CIK[%s], accession[%18s] "
            "form[%s] year[%s] qtr[%s] file[%s]" %
            (cik, accession, form_type, year, qtr, os.path.basename(filepath))
        )

        # --------------------------------------------------------------------------------
        # Get XBRL XML as the source
        # --------------------------------------------------------------------------------
        absolute_filepath = f"{input_xml_directory}{os.sep}{filepath}"
        try:
            xbrl = EdgarGAAP.load_xbrl(absolute_filepath)
        except RuntimeError as e:
            logging.error(
                "get_financial_statements(): failed to load XML[%s]. skipping "
                "CIK[%s], accession[%18s] form[%s] year[%s] qtr[%s]." %
                (cik, accession, form_type, year, qtr, os.path.basename(filepath))
            )
            return None

        # --------------------------------------------------------------------------------
        # Company (Registrant) name to verify if registrant element is in XBRL.
        # If not, there is a good chance the XBRL format is not as expected.
        # --------------------------------------------------------------------------------
        company_name = get_company_name(xbrl)
        if not company_name:
            logging.error(
                "get_financial_statements(): File[%s] CIK[%s] Accession[%s] "
                "has no registrant name found" %
                (os.path.basename(filepath), cik, accession)
            )

        # --------------------------------------------------------------------------------
        # Retrieve XML element attributes to extract the target XBRL XML elements
        # that are related to the filing report period.
        # --------------------------------------------------------------------------------
        date_from_xbrl_filename = get_date_from_xbrl_filename(filepath)
        attributes = get_attributes_to_select_target_fs_elements(
            soup=xbrl, form_type=form_type, date_from_xbrl_filename=date_from_xbrl_filename
        )

        # --------------------------------------------------------------------------------
        # Extract P/L (Income Statement) elements from the XBRL XML
        # --------------------------------------------------------------------------------
        pl = EdgarGAAP.get_PL(xbrl=xbrl, attributes=attributes)
        if pl is None or len(pl) <= 0:
            logging.info(
                "get_financial_statements(): skipping CIK[%s], accession[%18s] "
                "form[%s] year[%s] qtr[%s] file[%s] as no PL element found" %
                (cik, accession, form_type, year, qtr, os.path.basename(filepath))
            )
            return None

        logging.debug("get_financial_statements(): First row of the P/L:\n[%s]" % pl[0])

        # --------------------------------------------------------------------------------
        # Extract B/S elements from the XBRL XML
        # --------------------------------------------------------------------------------
        bs = EdgarGAAP.get_BS(xbrl=xbrl, attributes=attributes)
        if bs is None or len(bs) <= 0:
            logging.info(
                "get_financial_statements(): skipping CIK[%s], accession[%18s] "
                "form[%s] year[%s] qtr[%s] file[%s] as no BS element found" %
                (cik, accession, form_type, year, qtr, os.path.basename(filepath))
            )
            return None

        logging.debug("get_financial_statements(): First row of the B/S:\n[%s]" % bs[0])

        del xbrl
        fs = EdgarGAAP.prepend_columns(pl + bs, [cik, accession, form_type])
        return fs

    @staticmethod
    def get_nil_financial_statement(msg: dict, row) -> List[List[str]]:
        """Generate a NIL financial statement elements
        Args:
            msg: message
            row: |CIK|Form Type|Year|Quarter|Filepath|
        Returns:
            List of FS records with the format:
            |CIK|Accession|Form Type|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        filepath = row[DF_COLUMN_FILEPATH]
        if not (isinstance(filepath, str) and len(filepath) > 0):
            accession = "NA"
        else:
            accession = EdgarGAAP.get_accession_from_xbrl_filepath(filepath)
        cik = row[DF_COLUMN_CIK]
        form_type = row[DF_COLUMN_FORM_TYPE]

        return [
            [cik, accession, form_type, "NA", "NA", "NO FS ELEMENT FOUND", "-1", "NA", "0", "NA"]
        ]

    @staticmethod
    def create_df_FS(filings: List[List[str]], year: str, qtr: str):
        """Generate the dataframe for the financial statements
        Record in filings should have the format
        |CIK|Accession|Form_type|FS|Rep|Type|Name|Value|Unit|Decimals|Context|

        Args:
            filings: list of FS records
            year: report year
            qtr: report quarter
        Returns: Dataframe of the financial statement records in the format
                |CIK|Accession|Form_type|Year|Quarter|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        num_rows = len(filings)

        # --------------------------------------------------------------------------------
        # Build columns for dataframe
        # --------------------------------------------------------------------------------
        columns: List[str] = get_financial_element_columns()
        assert num_rows > 0, "create_df_FS(): No element. year[%s] qtr[%s]" % (year, qtr)
        assert columns[0] == DF_COLUMN_FS and columns[-1] == DF_COLUMN_CONTEXT, \
            "Unexpected columns. Verify if [%s] are correct order" % columns

        # Prepend columns to create a dataframe with the format:
        # |CIK|Accession|Form Type|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        for col in [DF_COLUMN_CIK, DF_COLUMN_ACCESSION, DF_COLUMN_FORM_TYPE][::-1]:
            columns.insert(0, col)

        # --------------------------------------------------------------------------------
        # Create dataframe + convert categorical
        # --------------------------------------------------------------------------------
        df_FS: pd.DataFrame = pd.DataFrame(filings, columns=columns)
        df_FS[DF_COLUMN_FORM_TYPE] = df_FS[DF_COLUMN_FORM_TYPE].astype('category')

        # --------------------------------------------------------------------------------
        # Insert year/qtr as categorical columns to generate the format:
        # |CIK|Accession|Year|Quarter|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        # Use int for year/qtr to limit the storage size 4 bytes for each column as
        # utf-8 string can take more bytes.
        # --------------------------------------------------------------------------------
        df_FS.insert(
            loc=df_FS.columns.get_loc(DF_COLUMN_ACCESSION)+1, column=DF_COLUMN_YEAR,
            value=pd.Categorical([int(year)]*num_rows)
        )
        df_FS.insert(
            loc=df_FS.columns.get_loc(DF_COLUMN_YEAR)+1, column=DF_COLUMN_QTR,
            value=pd.Categorical([int(qtr)]*num_rows)
        )

        logging.debug(
            "create_df_FS(): showing the first rows of df_FS created:\n[%s]" %
            df_FS.head(3)
        )
        return df_FS

    @ray.remote(num_returns=1)
    def worker(self, msg: dict) -> pd.DataFrame:
        """
        1. Load XBRL XML content.
        2. Parse the XML to extract Financial Statement (FS) elements.
        3. Create a dataframe.

        The incoming dataframe has the format where 'Filepath' is the relative path
        from the DIR_DATA_CSV_XML to XBRL XML
        |CIK|Company Name|Form Type|Date Filed|Year|Quarter|Filename|Filepath|

        [NOTE]: Need to pass "self" as worker.remote(self, msg) not worker.remote(msg).
        Python runtime automatically insert self if it is an instance method, but
        Ray "remote" proxy is a function, not class instance method.
        Alternatively make the remote method as static, however you cannot access
        instance/class members.

        Args:
            msg: Dictionary to data package of format {
                    "data": <dataframe>,
                    "year": <year of the filing>,
                    "qtr": <quarter of the filing>,
                    "log_level": <logging level>
            }
        Returns:
            Dataframe with the format
            |CIK|Year|Quarter|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        df = msg["data"]
        year: str = msg['year']
        qtr: str = msg['qtr']
        log_level:int = msg['log_level']
        assert isinstance(year, str) and year.isdecimal() and re.match(r"^[12][0-9]{3}$", year)
        assert isinstance(qtr, str) and qtr.isdecimal() and re.match(r"^[1-4]$", qtr)
        assert log_level in [10, 20, 30, 40]
        assert 'input_xml_directory' in msg, f"Directory to load XML not provided"

        # --------------------------------------------------------------------------------
        #  Logging setup for Ray as in https://docs.ray.io/en/master/ray-logging.html.
        #  In Ray, all the tasks and actors are executed remotely in the worker processes.
        #  Since Python logger module creates a singleton logger per process, loggers should
        #  be configured on per task/actor basis.
        # --------------------------------------------------------------------------------
        logging.basicConfig(level=log_level)
        logging.debug("worker(): starting with the task size is [%s]." % len(df))
        logging.debug(
            "worker(): showing the first rows from DF received for year[%s] qtr[%s]:\n[%s]"
            % (year, qtr, df.head(3))
        )

        # --------------------------------------------------------------------------------
        # Drop irrelevant columns
        # --------------------------------------------------------------------------------
        columns_to_drop = [DF_COLUMN_COMPANY, DF_COLUMN_DATE_FILED, DF_COLUMN_FILENAME]
        assert set(columns_to_drop).issubset(set(df.columns)), \
            "Invalid column included in %s" % columns_to_drop
        df.drop(columns_to_drop, axis=1, inplace=True)

        # --------------------------------------------------------------------------------
        # Get financial statements from a XBRL XML of each filing submission.
        # A company submits a filing that includes multiple statements e.g. BS, PL, CF.
        # From each filing (XBRL XML), get those statements and aggregate them.
        # --------------------------------------------------------------------------------
        filings = []  # List of SEC EDGAR filings
        for index, row in df.iterrows():
            try:
                EdgarGAAP.validate_year_qtr(row=row, year=year, qtr=qtr)
                statements = EdgarGAAP.get_financial_statements(msg, row)
                if not statements:
                    logging.error("worker(): no FS element found for row:\n[%s]" % row)
                    statements = EdgarGAAP.get_nil_financial_statement(msg, row)

                filings.extend(statements)
            except RuntimeError as e:
                logging.error("worker(): error [%s]." % e)
                logging.error("worker(): skipping row:\n[%s]" % row)

        assert isinstance(filings, list) and len(filings) > 0, "Must have rows."

        # --------------------------------------------------------------------------------
        # Generate dataframe of financial statements.
        # --------------------------------------------------------------------------------
        return EdgarGAAP.create_df_FS(filings, year=year, qtr=qtr)

    @staticmethod
    def compose_package_to_dispatch_to_worker(msg: dict, task: pd.DataFrame):
        year = msg['year']
        qtr = msg['qtr']
        input_xml_directory = msg["input_xml_directory"]
        log_level = msg['log_level']

        return {
            "data": task,
            "year": year,
            "qtr": qtr,
            "input_xml_directory": input_xml_directory,
            "log_level": log_level
        }

    @staticmethod
    def report_result(msg, result: pd.DataFrame, need_result_data=False) -> str:
        # --------------------------------------------------------------------------------
        # List failed records with 'Filepath' column being None as failed to get XBRL
        # --------------------------------------------------------------------------------
        record_counts = f"Processed [{len(result)}]."
        report = record_counts

        return report


# --------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------
if __name__ == "__main__":
    EdgarGAAP().main()
