"""ETL asset base implementation for Dagster pipelines in AIF.

This module contains a base class that can be used to create ETL (Extract, Transform, Load) assets
within Dagster pipelines. It provides a standardized structure for implementing ETL processes
with proper logging and error handling.
"""

from abc import abstractmethod, ABC

import pandas as pd
import dagster as dg

import aif.common.aif.src.aif_logging as logging


class ETLAsset(ABC):
    """Abstract base class that implements the main ETL flow logic.

    This class provides a structured approach to implementing ETL processes within Dagster assets.
    Subclasses must implement the extract, transform, and load methods to define the specific
    ETL behavior.

    Attributes:
        fail_on_missing_entries: Flag to determine whether the ETL process should fail if
            entries cannot be loaded into the target system.
    """

    def __init__(self, fail_on_missing_entries: bool) -> None:
        """Initialize the ETL asset.

        Parameters:
            fail_on_missing_entries: If True, the ETL process will raise an exception when
                entries cannot be loaded into the target system.
        """
        self.fail_on_missing_entries: bool = fail_on_missing_entries

    def run(self) -> dg.MaterializeResult:
        """Run the complete ETL flow for the current job.

        This method orchestrates the extract, transform, and load steps of the ETL process,
        with appropriate logging at each stage.

        Returns:
            dg.MaterializeResult: A Dagster materialization result containing metadata about
                the ETL operation, including the total number of datapoints processed and
                the number of missing datapoints.

        Raises:
            RuntimeError: If no data could be extracted or if entries could not be loaded
                and fail_on_missing_entries is True.
        """
        logging.get_aif_logger(__name__).debug("Start etl ...")
        logging.get_aif_logger(__name__).debug(" Extracting data...")
        extract_df = self.extract()

        if len(extract_df) == 0:
            raise RuntimeError("Could not extract data.")

        logging.get_aif_logger(__name__).debug(" Transforming data...")
        transform_df = self.transform(df=extract_df)

        logging.get_aif_logger(__name__).debug(" Loading data...")
        missing: int = self.load(df=transform_df)

        if missing > 0 and self.fail_on_missing_entries:
            raise RuntimeError(f"{missing} entries could not be loaded into the database.")

        return dg.MaterializeResult(
            metadata={
                "Total datapoints": dg.MetadataValue.int(len(transform_df)),
                "Missing datapoints": dg.MetadataValue.int(missing),
            },
        )

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from the source system.

        This method must be implemented by subclasses to define how data is extracted
        from the source system.

        Returns:
            pd.DataFrame: The extracted data as a pandas DataFrame.
        """

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the extracted data.

        This method must be implemented by subclasses to define how the extracted data
        is transformed into the desired format.

        Parameters:
            df: The DataFrame containing the extracted data.

        Returns:
            pd.DataFrame: The transformed data as a pandas DataFrame.
        """

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Load the transformed data into the target system.

        This method must be implemented by subclasses to define how the transformed data
        is loaded into the target system.

        Parameters:
            df: The DataFrame containing the transformed data to be loaded.

        Returns:
            int: The number of rows that could not be loaded into the target system.
        """
