"""Database interface module for the AIF framework.

This module provides a high-level interface for database operations within the AIF framework.
It includes functionality for:
1. Connecting to different database types (PostgreSQL, Snowflake)
2. Executing SQL queries and statements
3. Inserting data from DataFrames into database tables
4. Creating and managing database schemas, tables, and views

The module is designed to abstract away the differences between database implementations
and provide a consistent interface for all database operations in the AIF framework.
"""

import json
from dataclasses import dataclass
from typing import Optional, Any

import pandas as pd

from aif.common.aif.src import aif_logging as logging
from aif.common.aif.src.config import settings
from aif.common.aif.src.data_interfaces.db_impl import DBImpl
from aif.common.aif.src.data_interfaces.pg_impl import PGImpl
from aif.common.aif.src.utils.dict_utils import safe_merge_dicts


def dbfunc(f):
    """Decorator for database interaction functions in the DBInterface class.

    This decorator handles common database operation patterns, including:
    1. Checking for an active connection
    2. Creating and managing database cursors
    3. Handling exceptions and rolling back transactions
    4. Ensuring proper cursor cleanup

    The main aim of the decorator is to get the cursor from the current DB connection
    and to ensure that the cursor is closed in the end, even if the called db function
    raises an exception.

    Args:
        f: The database function to be decorated

    Returns:
        A wrapped function that handles cursor management and error handling

    Raises:
        RuntimeError: If there is no active database connection
    """

    def db_func_wrapper(self, *args, **kwargs):
        if self.conn is None:
            raise RuntimeError("""No active connection. Use 'with DBInterface() as db: ...' """)

        # cur: Optional[psycopg.Cursor | Connection] = None
        cur = None

        try:
            cur = self.conn.cursor()
            result = f(self, cur, *args, **kwargs)
        except Exception as e:
            logging.get_aif_logger(__name__).warning("Error in database function - Rollback: %s", str(e))
            self.conn.rollback()
            raise e
        finally:
            if cur is not None:
                cur.close()
        return result

    return db_func_wrapper


@dataclass
class DBResult:
    """Data class for database operation results.

    This class holds the result of a database operation performed by the DBInterface.
    It includes the SQL statement that was executed, the resulting DataFrame (if any),
    and additional metadata about the operation.

    Attributes:
        sql_stmt (str): The SQL statement that was executed
        metadata (Optional[dict]): Additional metadata about the operation
        result_df (Optional[pd.DataFrame]): The resulting DataFrame, if applicable
    """

    sql_stmt: str
    metadata: Optional[dict] = None
    result_df: Optional[pd.DataFrame] = None


class DBInterface:
    """High-level interface for database operations in the AIF framework.

    This class provides a unified interface for interacting with different database types
    (PostgreSQL, Snowflake) in the AIF framework. It must be used as a context manager
    to ensure proper connection management:

    ```python
    with DBInterface(db_cfg='database_config') as db:
        # Perform database operations
    ```

    By using the context manager pattern, a new connection to the database is created and
    properly closed, even when an exception occurs during execution.

    If no parameter is given, DBInterface will connect to the database specified under
    settings["common"]["default_db"]. To connect to a different database, provide the
    configuration name as defined in the settings.

    Attributes:
        conn: The active database connection
        db_cfg (str): The database configuration name from settings
        db_impl (DBImpl): The specific database implementation (PG, SF, etc.)

    Raises:
        RuntimeError: If no database configuration is provided
        ValueError: If an unknown database type is specified in the configuration
    """

    def __init__(self, db_cfg: str):
        # self.conn: Optional[psycopg.connection.Connection] = None
        self.conn = None
        self.db_cfg: str = db_cfg

        # Validate database configuration
        if self.db_cfg is None:
            logging.get_aif_logger(__name__).error(
                "No database configuration given. Please use DBInterface(db_cfg=...)"
            )
            raise RuntimeError("No database configuration given. Please use DBInterface(db_cfg=...)")

        self.db_impl: DBImpl
        if settings[self.db_cfg]["type"].upper() == "POSTGRES":
            self.db_impl = PGImpl()
        elif settings[self.db_cfg]["type"].upper() == "SNOWFLAKE":
            raise RuntimeError("Snowflake is not available in this version")
        else:
            db_type = settings[self.db_cfg]["db_type"]
            logging.get_aif_logger(__name__).error("Unknown database type: %s", db_type)
            raise ValueError(f"Unknown database type: {db_type}")

    def __enter__(self):
        """Initialize the database connection when entering the context manager.

        This method is called when the context manager is entered using the 'with' statement.
        It establishes a connection to the database using the appropriate database implementation
        based on the configuration.

        Returns:
            DBInterface: The initialized database interface instance with an active connection

        Raises:
            Exception: Any exception that occurs during connection establishment is logged
                      with detailed information and re-raised
        """
        logging.get_aif_logger(__name__).debug("Connecting to database: %s", self.db_cfg)
        db_settings = settings[self.db_cfg]
        try:
            self.conn = self.db_impl.get_connection(db_settings)
            logging.get_aif_logger(__name__).debug("Connection to database established...")

            return self
        except Exception as e:
            logging.get_aif_logger(__name__).error("Could not connect to database %s: %s", self.db_cfg, str(e))
            logging.get_aif_logger(__name__).error("Settings:\n%s", json.dumps(db_settings))
            raise e

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the database connection when exiting the context manager.

        This method is called when the context manager is exited. It ensures that
        the database connection is properly closed, even if an exception occurred
        during the execution of the code block.

        Args:
            exc_type: The exception type if an exception was raised in the context, None otherwise
            exc_val: The exception value if an exception was raised in the context, None otherwise
            exc_tb: The traceback if an exception was raised in the context, None otherwise

        Raises:
            RuntimeError: If no connection is found when trying to close it
        """
        if self.conn is None:
            raise RuntimeError("No connection found while exit connection.")

        self.conn.close()
        self.conn = None

        logging.get_aif_logger(__name__).debug("Connection to database was closed.")

    # High level methods, that encapsulate the SQL logic

    def create_schema(self, schema_name: str, schema_comment: str) -> DBResult:
        """Create a schema in the database if it doesn't already exist.

        This method creates a new schema in the database and sets a comment on it.
        If the schema already exists, it will not be recreated, but the comment
        will still be updated.

        Args:
            schema_name (str): The name of the schema to create
            schema_comment (str): A description or comment for the schema

        Returns:
            DBResult: A result object containing the executed SQL statements

        Note:
            This method executes two SQL statements: one to create the schema
            and another to set the comment. The statements are executed in a
            single transaction.
        """
        # Format the schema name according to the database conventions
        # formatted_schema_name = self.db_impl.format_identifier(schema_name)

        sql_schema = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
        sql_comment = f"""COMMENT ON SCHEMA {schema_name} IS '{schema_comment}';"""

        # pylint: disable=no-value-for-parameter
        self._plain_sql_execution(sql_stmt=sql_schema, commit=False)
        self._plain_sql_execution(sql_stmt=sql_comment, commit=True)
        # pylint: enable=no-value-for-parameter

        logging.get_aif_logger(__name__).info("Created schema: %s", schema_name)

        return DBResult(sql_stmt=sql_schema + "\n " + sql_comment)

    def drop_table(self, table_name: str) -> DBResult:
        """Drop a table from the database if it exists.

        This method removes a table from the database. If the table does not exist,
        no error is raised.

        Args:
            table_name (str): The name of the table to drop, can include schema name
                             (e.g., 'schema_name.table_name')

        Returns:
            DBResult: A result object containing the executed SQL statement
        """
        sql_drop = f"DROP TABLE IF EXISTS {table_name}"
        self._plain_sql_execution(sql_stmt=sql_drop, commit=True)  # pylint: disable=no-value-for-parameter

        logging.get_aif_logger(__name__).info("Dropped table %s", table_name)

        return DBResult(sql_stmt=sql_drop)

    def drop_view(self, view_name: str, materialized: bool) -> DBResult:
        """Drop a view from the database if it exists.

        This method removes a view (either regular or materialized) from the database.
        If the view does not exist, no error is raised.

        Args:
            view_name (str): The name of the view to drop, can include schema name
                            (e.g., 'schema_name.view_name')
            materialized (bool): If True, drops a materialized view; otherwise, drops a regular view

        Returns:
            DBResult: A result object containing the executed SQL statement
        """
        msg_mat = " materialized" if materialized else ""

        sql_drop = f"DROP{msg_mat.upper()} VIEW IF EXISTS {view_name}"
        self._plain_sql_execution(sql_stmt=sql_drop, commit=True)  # pylint: disable=no-value-for-parameter

        logging.get_aif_logger(__name__).info("Dropped %s view %s", msg_mat, view_name)

        return DBResult(sql_stmt=sql_drop)

    def refresh_mat_view(self, view_name: str) -> DBResult:
        """Refresh a materialized view to update its contents.

        This method refreshes the data in a materialized view by re-executing
        the view's defining query. The specific SQL syntax used depends on the
        database implementation.

        Args:
            view_name (str): The name of the materialized view to refresh, can include
                            schema name (e.g., 'schema_name.view_name')

        Returns:
            DBResult: A result object containing the executed SQL statement
        """
        # Use the database-specific implementation for refreshing materialized views
        sql_refresh = self.db_impl.get_refresh_materialized_view_stmt(view_name)
        self._plain_sql_execution(sql_stmt=sql_refresh, commit=True)  # pylint: disable=no-value-for-parameter

        logging.get_aif_logger(__name__).info("Refreshed view %s", view_name)

        return DBResult(sql_stmt=sql_refresh)

    def execute_insert(
        self,
        data_df: pd.DataFrame,
        schema: str,
        table_name: str,
        filename: Optional[str] = None,
        parameters=None,
        warn_on_missing=True,
    ) -> DBResult:
        """Insert data from a DataFrame into a database table.

        This method inserts data from a pandas DataFrame into a database table.
        It can use either a direct insert approach or a custom SQL statement from a file.
        The method verifies the number of rows actually inserted and reports any discrepancies.

        NOTE: filename will be transformed to lower case, since placeholders like DWH_NAME are often upper case, which
              does not align with python convention.

        Args:
            data_df (pd.DataFrame): The DataFrame containing the data to insert
            schema (str): The database schema containing the target table
            table_name (str): The name of the target table
            filename (Optional[str], optional): Path to a SQL file containing a custom insert
                                              statement. If None, a direct insert is used.
                                              Defaults to None.
            parameters (dict, optional): Parameters to substitute in the SQL file.
                                       Defaults to None.
            warn_on_missing (bool, optional): Whether to log a warning if some rows
                                            were not inserted. Defaults to True.

        Returns:
            DBResult: A result object with metadata containing the number of missing rows
                     (rows that were not successfully inserted)

        """
        # Count before insert
        sql_cnt_stmt = f"select count(*) from {schema}.{table_name}"
        res_cnt_before = self._plain_sql_query(sql_stmt=sql_cnt_stmt)  # pylint: disable=no-value-for-parameter
        cnt_before: int = res_cnt_before.result_df.iloc[0, 0]

        if filename:
            # Prepare SQL statement
            sql_stmt = self._get_sql_from_file(filename=filename, parameters=parameters)
            sql_stmt = sql_stmt.replace("{SCHEMA_NAME}", schema)
            sql_stmt = sql_stmt.replace("{TABLE_NAME}", table_name)

            sql_insert_to_execute = sql_stmt
            # Insert data
            if len(data_df.index) > 0:
                # pylint: disable=no-value-for-parameter
                self._batch_insert_stmt(
                    sql_stmt=sql_insert_to_execute, data_df=data_df, schema=schema, table_name=table_name
                )
                # pylint: enable=no-value-for-parameter
        else:
            self._batch_insert(  # pylint: disable=no-value-for-parameter
                data_df=data_df, schema=schema, table_name=table_name
            )

        # Count after insert
        res_cnt_after = self._plain_sql_query(sql_stmt=sql_cnt_stmt)  # pylint: disable=no-value-for-parameter
        cnt_after: int = res_cnt_after.result_df.iloc[0, 0]

        # Verify and log
        delta = cnt_after - cnt_before

        logging.get_aif_logger(__name__).info(
            """Added %s/%s datapoints to table '%s'.'""", delta, len(data_df), f"{schema}.{table_name}"
        )

        missing: int = len(data_df) - delta
        if missing > 0 and warn_on_missing:
            logging.get_aif_logger(__name__).warning("Missing %s datapoints.", missing)

        return DBResult(sql_stmt="Batch insert", metadata={"missing": int(missing)})

    def execute_query(self, sql_stmt: str) -> DBResult:
        """Execute a SQL query and return the results as a DataFrame.

        This method executes a SQL query (typically a SELECT statement) and returns
        the results in a pandas DataFrame.

        Args:
            sql_stmt (str): The SQL query to execute

        Returns:
            DBResult: A result object containing the executed SQL statement and
                     the query results as a DataFrame
        """
        # pylint: disable=no-value-for-parameter
        return self._plain_sql_query(sql_stmt=sql_stmt)
        # pylint: enable=no-value-for-parameter

    def execute_query_from_file(self, filename: str, parameters=None) -> DBResult:
        """Execute a SQL query from a file and return the results as a DataFrame.

        This method reads a SQL query from a file, substitutes any parameters,
        and executes it. The results are returned in a pandas DataFrame.

        Args:
            filename (str): Path to the SQL file containing the query
            parameters (dict, optional): Dictionary of parameter values to substitute
                                       in the SQL query. Defaults to None.

        Returns:
            DBResult: A result object containing the executed SQL statement and
                     the query results as a DataFrame

        Note:
            Parameters in the SQL file should be specified in the format {PARAM_NAME}.
            For example, if the SQL file contains "SELECT * FROM {TABLE_NAME}",
            and parameters is {'TABLE_NAME': 'my_table'}, the executed query will be
            "SELECT * FROM my_table".
        """
        sql_stmt = self._get_sql_from_file(filename=filename, parameters=parameters)

        return self.execute_query(sql_stmt=sql_stmt)

    def execute_statement(self, sql_stmt: str) -> DBResult:
        """Execute a SQL statement and commit the changes.

        This method executes a SQL statement (typically a DDL or DML statement)
        and commits the changes to the database.

        Args:
            sql_stmt (str): The SQL statement to execute

        Returns:
            DBResult: A result object containing the executed SQL statement
        """
        return self._plain_sql_execution(sql_stmt=sql_stmt, commit=True)  # pylint: disable=no-value-for-parameter

    def execute_statement_from_file(self, filename: str, parameters=None) -> DBResult:
        """Execute a SQL statement from a file and commit the changes.

        This method reads a SQL statement from a file, substitutes any parameters,
        executes it, and commits the changes to the database.

        NOTE: Files with multiple statements can separate them with "-- AIF: NEW_STATEMENT --" (without "")  to execute
        them separately. This is necessary for e.g. Snowflake. If more than one statement is executed, no DataFrames
        should be returned.

        Args:
            filename (str): Path to the SQL file containing the statement
            parameters (dict, optional): Dictionary of parameter values to substitute
                                       in the SQL statement. Defaults to None.

        Returns:
            DBResult: A result object containing the executed SQL statement
        """
        sql_content = self._get_sql_from_file(filename=filename, parameters=parameters)

        # Split the SQL content into separate statements using the delimiter
        statements = sql_content.split("-- AIF: NEW_STATEMENT --")

        # Filter out empty statements
        statements = [stmt.strip() for stmt in statements if stmt.strip()]

        # If there are no statements, return an empty result
        if not statements:
            return DBResult(sql_stmt="")

        # If there's only one statement, execute it normally
        if len(statements) == 1:
            return self.execute_statement(sql_stmt=statements[0])

        # Execute each statement individually
        results = []
        merged_metadata: dict[str, Any] = {}
        combined_sql = ""

        for stmt in statements:
            result = self.execute_statement(sql_stmt=stmt)
            results.append(result)

            # Combine SQL statements for reference
            combined_sql += stmt + ";\n\n"

            # Safely merge metadata
            if result.metadata:
                try:
                    merged_metadata = safe_merge_dicts(merged_metadata, result.metadata)
                except RuntimeError as e:
                    logging.get_aif_logger(__name__).warning(
                        "Could not merge metadata from multiple SQL statements: %s", str(e)
                    )

        # For multiple statements, create a result with combined SQL and merged metadata,
        # but without a result_df
        return DBResult(sql_stmt=combined_sql.strip(), metadata=merged_metadata)

    def call_method(self, method_name: str, *args, **kwargs) -> DBResult:
        """Call a method of the DBInterface class by name.

        This method provides a way to dynamically call other methods of the DBInterface
        class by their name. It is useful for generic database operations where the
        specific method to call is determined at runtime.

        Args:
            method_name (str): The name of the method to call
            *args: Positional arguments to pass to the method
            **kwargs: Keyword arguments to pass to the method

        Returns:
            DBResult: The result returned by the called method

        Raises:
            ValueError: If attempting to call a private method (name starts with '_')
            AttributeError: If the specified method does not exist
            TypeError: If the called method does not return a DBResult
        """
        logging.get_aif_logger(__name__).debug("Try to execute generic method %s", method_name)
        if method_name.startswith("_"):
            raise ValueError("No private methods should be called.")

        method = getattr(self, method_name, None)
        if method and callable(method):
            result = method(*args, **kwargs)
            if isinstance(result, DBResult):
                return result

            raise TypeError(f"Method {method_name} did not return a DBResult.")

        raise AttributeError(f"Method {method_name} not found in DBInterface.")

    # Private methods to do the real work

    @staticmethod
    def _get_sql_from_file(filename: str, parameters=None) -> str:
        """Read a SQL statement from a file and substitute parameters.

        This method reads the contents of a SQL file and replaces any parameter
        placeholders with their corresponding values.

        Note:
            The filename is converted to lower case, since placeholders like DWH_NAME are often upper case, which
            does not align with python convention.

        Args:
            filename (str): Path to the SQL file, relative to the project path
            parameters (dict, optional): Dictionary of parameter values to substitute
                                       in the SQL statement. Defaults to None.

        Returns:
            str: The SQL statement with parameters substituted

        Raises:
            ValueError: If the project path is not defined in settings
            FileNotFoundError: If the SQL file does not exist
        """
        if parameters is None:
            parameters = {}

        if (path := str(settings["path"])) is None:
            raise ValueError("path is not in settings")

        filename_lower = filename.lower()

        if filename_lower == filename:
            logging.get_aif_logger(__name__).info("SQL file name was transformed to lower case: %s", filename_lower)

        logging.get_aif_logger(__name__).debug("Reading statement from file: %s", filename_lower)
        sql_stmt = ""
        with open(str(path) + filename_lower, mode="r", encoding="utf8") as f:
            sql_stmt = f.read()

        for p, pv in parameters.items():
            sql_stmt = sql_stmt.replace("{{ " + p + " }}", pv)

        return sql_stmt

    @dbfunc
    def _plain_sql_query(self, cur, sql_stmt: str) -> DBResult:
        """Execute a SQL query and return the results as a DataFrame.

        This method executes a SQL query statement and fetches all results,
        converting them into a pandas DataFrame.

        Args:
            cur: The database cursor to use for execution
            sql_stmt (str): The SQL query to execute

        Returns:
            DBResult: A result object containing the executed SQL statement and
                     the query results as a DataFrame

        Note:
            This method is decorated with @dbfunc, which handles cursor management
            and exception handling.
        """
        logging.get_aif_logger(__name__).info("Executing Statement:\n%s", sql_stmt)

        cur.execute(sql_stmt)

        col_names = [desc[0] for desc in cur.description]
        result_df = pd.DataFrame(list(cur), columns=col_names)

        return DBResult(sql_stmt=sql_stmt, result_df=result_df)

    @dbfunc
    def _plain_sql_execution(self, cur, sql_stmt: str, commit: bool) -> DBResult:
        """Execute a SQL statement with optional commit.

        This method executes a SQL statement (typically a DDL or DML statement)
        on a new cursor and optionally commits the changes.

        Args:
            cur: The database cursor to use for execution
            sql_stmt (str): The SQL statement to execute
            commit (bool): Whether to commit the transaction after execution

        Returns:
            DBResult: A result object containing the executed SQL statement

        Note:
            This method is decorated with @dbfunc, which handles cursor management
            and exception handling.
        """
        logging.get_aif_logger(__name__).info("Executing Statement:\n%s", sql_stmt)

        cur.execute(sql_stmt)

        if commit:
            if self.conn is not None:
                self.conn.commit()
            else:
                # Should never happen, but for proper linting and who knows what crazy things can happen
                raise RuntimeError("No active connection. Use 'with DBInterface() as db: ...'")

        return DBResult(sql_stmt=sql_stmt)

    @dbfunc
    def _batch_insert_stmt(self, cur, sql_stmt: str, data_df: pd.DataFrame, schema: str, table_name: str) -> None:
        """Insert data using a custom SQL statement.

        This method inserts data from a DataFrame using a custom SQL statement.
        It converts the DataFrame to a list of tuples and uses executemany for
        efficient batch insertion.

        Args:
            cur: The database cursor to use for execution
            sql_stmt (str): The SQL insert statement with parameter placeholders
            data_df (pd.DataFrame): The DataFrame containing the data to insert
            schema (str): The database schema containing the target table
            table_name (str): The name of the target table

        Note:
            This method is decorated with @dbfunc, which handles cursor management
            and exception handling.
        """
        logging.get_aif_logger(__name__).debug("Executing Statement:\n%s", sql_stmt)
        logging.get_aif_logger(__name__).debug("Number of datapoints to insert: %s", len(data_df))

        # Use the database-specific implementation for batch inserts
        self.db_impl.execute_batch_insert_stmt(
            cur=cur, sql_stmt=sql_stmt, data_df=data_df, schema=schema, table_name=table_name
        )
        if self.conn is not None:
            self.conn.commit()
        else:
            # Should never happen, but for proper linting and who knows what crazy things can happen
            raise RuntimeError("No active connection. Use 'with DBInterface() as db: ...'")

    @dbfunc
    def _batch_insert(self, cur, data_df: pd.DataFrame, schema: str, table_name: str) -> None:
        """Insert data using the database implementation's native method.

        This method inserts data from a DataFrame using the database-specific
        implementation's native batch insert method. This is typically more
        efficient than using a generic SQL insert statement.

        Args:
            cur: The database cursor to use for execution
            data_df (pd.DataFrame): The DataFrame containing the data to insert
            schema (str): The database schema containing the target table
            table_name (str): The name of the target table

        Note:
            This method is decorated with @dbfunc, which handles cursor management
            and exception handling.
        """
        logging.get_aif_logger(__name__).debug(
            "Number of datapoints to insert into %s.%s: %s", schema, table_name, len(data_df)
        )

        self.db_impl.execute_batch_insert(cur=cur, data_df=data_df, schema=schema, table_name=table_name)

        if self.conn is not None:
            self.conn.commit()
        else:
            # Should never happen, but for proper linting and who knows what crazy things can happen
            raise RuntimeError("No active connection. Use 'with DBInterface() as db: ...'")
