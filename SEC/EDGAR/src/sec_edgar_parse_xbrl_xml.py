import logging
import re
from typing import (
    List,
    Callable
)

import bs4
import pandas as pd
import ray
from bs4 import BeautifulSoup

from sec_edgar_base import (
    EdgarBase
)
from sec_edgar_constant import (
    # SEC Form Types
    # EDGAR
    DF_COLUMN_CIK,
    DF_COLUMN_COMPANY,
    DF_COLUMN_FORM_TYPE,
    DF_COLUMN_DATE_FILED,
    DF_COLUMN_FILENAME,
    DF_COLUMN_FILEPATH,
    DF_COLUMN_FS,
    DF_COLUMN_YEAR,
    DF_COLUMN_QTR,
    DF_COLUMN_CONTEXT,
    # Financial statement types
    # Financial Statement element types
    #
    # PL
    # BS
)
from xbrl_gaap_function import (
    get_attributes_to_select_target_fs_elements,
    get_financial_element_columns,
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


class EdgarGapp(EdgarBase):
    # ================================================================================
    # Init
    # ================================================================================
    def __init__(self):
        super().__init__()

    # ================================================================================
    # Logic
    # ================================================================================
    @staticmethod
    def validate_year_qtr(row, year, qtr):
        assert year == row[DF_COLUMN_YEAR], \
            "Year mismatch. msg['year'] is [%s] but row['year'] is [%s]." % \
            (year, row[DF_COLUMN_YEAR])
        assert qtr == row[DF_COLUMN_QTR], \
            "Year mismatch. msg['qtr'] is [%s] but row['qtr'] is [%s]." % \
            (qtr, row[DF_COLUMN_QTR])

        if not row[DF_COLUMN_FILEPATH]:
            logging.error(
                "Skipping CIK [%s] Year [%s] Qtr [%s] as no 'Filepath in row: \n[%s]" %
                (row[DF_COLUMN_CIK], row[DF_COLUMN_YEAR], row[DF_COLUMN_QTR], row)
            )

    @staticmethod
    def load_from_xml(filepath:str) -> str:
        """Load the XML contents from the filepath
        Args:
            filepath: path to the XML file
        Returns: XML content
        """
        try:
            with open(filepath, "r") as f:
                content = f.read()
        except OSError as e:
            logging.error("load_from_xml():failed to read [%s] for [%s]." % (filepath, e))
            raise RuntimeError("load_from_xml()") from e
        pass
        return content

    def get_PL(
            self, source: bs4.BeautifulSoup, attributes: dict, cik: str
    ) -> List[List[str]]:
        """Generate PL records.
        Args:
            source: XBRL XML source
            attributes: XML attributes to match the XML elements
            cik: CIK of the filing compnay
        Returns:
            List of FS records with the format:
            |CIK|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        return sum([[cik] + f(source) for f in PL_FUNCTIONS], [])

    def get_BS(
            self, source: bs4.BeautifulSoup, attributes: dict, cik: str
    ) -> List[List[str]]:
        """Generate BS records.
        Args:
            source: XBRL XML source
            attributes: XML attributes to match the XML elements
            cik: CIK of the filing compnay
        Returns:
            List of FS records with the format:
            |CIK|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        return sum([[cik] + f(source) for f in BS_FUNCTIONS], [])

    def generate_financial_statement(self, row: dict) -> List[List[str]]:
        """Generate a list of financial statement elements
        Args:
            row: |CIK|Form Type|Date Filed|Year|Quarter|Filename|Filepath|
        Returns:
            List of FS records with the format:
            |CIK|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        """
        assert isinstance(row, dict)
        filepath = row[DF_COLUMN_FILEPATH]
        cik = row[DF_COLUMN_CIK]
        form_type = row[DF_COLUMN_FORM_TYPE]

        xml = self.load_from_xml(filepath)
        source = BeautifulSoup(xml, 'html.parser')

        attributes = get_attributes_to_select_target_fs_elements(
            soup=soup, form_type=form_type
        )
        pl = self.get_PL(source=source, )

        del soup
        return pl + bs

    @staticmethod
    def create_df_FS(financial_statements: List[List[str]], year: str, qtr: str):
        """Generate the dataframe for the financial statements
        Record in financial_statements should have the format
        |CIK|Year|Quarter|FS|Rep|Type|Name|Value|Unit|Decimals|Context|

        Args:
            financial_statements: list of FS records
            year: report year
            qtr: report quarter
        Returns: Dataframe of the financial statement records
        """
        columns = get_financial_element_columns()
        assert columns[0] == DF_COLUMN_FS and columns[-1] == DF_COLUMN_CONTEXT, \
            "Unexpected columns. Verify if [%s] are correct order" % columns

        # --------------------------------------------------------------------------------
        # Append DF_COLUMN_CIK and create a dataframe with the format:
        # |CIK|Year|Quarter|FS|Rep|Type|Name|Value|Unit|Decimals|Context|
        # --------------------------------------------------------------------------------
        columns.insert(DF_COLUMN_CIK, 0)
        df_FS = pd.DataFrame(financial_statements, columns=columns)

        # --------------------------------------------------------------------------------
        # Insert year/qtr as categorical columns
        # --------------------------------------------------------------------------------
        year: int = int(year)
        qtr: int = int(qtr)
        df_FS.insert(
            loc=df_FS.columns.get_loc("CIK")+1, column='Year',
            value=pd.Categorical([year]*len(df_FS))
        )
        df_FS.insert(
            loc=df_FS.columns.get_loc("Year")+1, column='Quarter',
            value=pd.Categorical([qtr]*len(df_FS))
        )
        assert df_FS is not None and len(df_FS) > 0, "Invalid df_FS"
        logging.debug("worker(): df[0]:\n %s", df_FS.head(1))

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

        # --------------------------------------------------------------------------------
        #  Logging setup for Ray as in https://docs.ray.io/en/master/ray-logging.html.
        #  In Ray, all of the tasks and actors are executed remotely in the worker processes.
        #  Since Python logger module creates a singleton logger per process, loggers should
        #  be configured on per task/actor basis.
        # --------------------------------------------------------------------------------
        logging.basicConfig(level=log_level)
        logging.info("worker(): task size is %s" % len(df))

        # --------------------------------------------------------------------------------
        # Drop irrelevant columns
        # --------------------------------------------------------------------------------
        columns_to_drop = [DF_COLUMN_COMPANY, DF_COLUMN_DATE_FILED, DF_COLUMN_FILENAME]
        df.drop(columns_to_drop, axis=1, inplace=True)

        # --------------------------------------------------------------------------------
        # Generate financial statements from XBRL XML
        # --------------------------------------------------------------------------------
        financial_statements = []
        for index, row in df.iterrows():
            self.validate_year_qtr(row=row, year=year, qtr=qtr)
            fs = self.generate_financial_statement(row)
            financial_statements.extend(fs)

        # --------------------------------------------------------------------------------
        # Generate dataframe of financial statements.
        # --------------------------------------------------------------------------------
        return self.create_df_FS(financial_statements, year=year, qtr=qtr)
