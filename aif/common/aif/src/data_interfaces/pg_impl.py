"""
PostgreSQL database implementation module.

This module provides a PostgreSQL-specific implementation of the DBImpl interface.
It handles connection management, query execution, and data manipulation operations
specific to PostgreSQL database systems.
"""

import pandas as pd
import psycopg

from aif.common.aif.src.data_interfaces.db_impl import DBImpl


class PGImpl(DBImpl):
    """
    PostgreSQL implementation of the DBImpl interface.

    This class provides PostgreSQL-specific implementations of the abstract methods
    defined in the DBImpl base class. It handles connection management, SQL statement
    formatting, and data manipulation operations tailored for PostgreSQL database systems.
    """

    def get_connection(self, db_settings):
        """
        Establish and return a connection to the PostgreSQL database.

        This method creates a connection string using the provided database settings
        and establishes a connection to the PostgreSQL database.

        Args:
            db_settings (dict): Configuration parameters required to connect to PostgreSQL.
                Required keys:
                - user: PostgreSQL username
                - password: PostgreSQL password
                - host: Database server hostname or IP address
                - port: Database server port
                - db_name: Database name to connect to

        Returns:
            psycopg.Connection: A PostgreSQL connection object with autocommit set to False.
        """
        credentials: str = f"""{db_settings["user"]}:{db_settings["password"]}"""
        host: str = f"{db_settings["host"]}:{db_settings["port"]}"
        connection_str: str = f"""postgresql://{credentials}@{host}/{db_settings["db_name"]}""".replace(" ", "")

        return psycopg.connect(connection_str, autocommit=False)

    def get_refresh_materialized_view_stmt(self, view_name: str) -> str:
        """
        Return the SQL statement to refresh a materialized view in PostgreSQL.

        Args:
            view_name (str): The name of the materialized view to refresh.

        Returns:
            str: SQL statement that can be executed to refresh the specified materialized view.
        """
        return f"REFRESH MATERIALIZED VIEW {view_name}"

    def get_parameter_placeholder(self, param_index: int) -> str:
        """
        Return the parameter placeholder for PostgreSQL.

        PostgreSQL uses positional parameters in the format $1, $2, etc.

        Args:
            param_index (int): The index of the parameter in the query.

        Returns:
            str: The PostgreSQL parameter placeholder (e.g., '$1', '$2').
        """
        return f"${param_index}"

    def wrap_sql_stmt(self, sql_stmt: str) -> str:
        """
        PostgreSQL doesn't require special wrapping for multiple statements,
        so this method simply returns the original SQL statement.

        Args:
            sql_stmt (str): The SQL statement(s) to be wrapped.

        Returns:
            str: The original SQL statement unchanged.
        """
        return sql_stmt

    def execute_batch_insert_stmt(
        self, cur, sql_stmt: str, data_df: pd.DataFrame, schema: str, table_name: str
    ) -> None:
        """
        Execute a batch insert operation using PostgreSQL.

        This method converts the DataFrame to a list of tuples and uses PostgreSQL's
        executemany functionality for efficient batch insertion. The SQL statement
        can contain ON CONFLICT constraints for handling duplicate entries.

        Args:
            cur: PostgreSQL cursor object used for executing SQL commands.
            sql_stmt (str): The SQL insert statement with parameter placeholders.
            data_df (pd.DataFrame): Pandas DataFrame containing the data to be inserted.
            schema (str): The database schema where the target table is located (not used in this implementation).
            table_name (str): The name of the table to insert data into (not used in this implementation).
        """
        data_values: list[tuple] = [tuple(a) for a in data_df.values]
        cur.executemany(sql_stmt, data_values)

    def execute_batch_insert(self, cur, data_df: pd.DataFrame, schema: str, table_name: str) -> None:
        """
        Execute a batch insert operation using PostgreSQL.

        This method converts the DataFrame to a list of tuples and uses PostgreSQL's
        executemany functionality for efficient batch insertion.

        Args:
        cur: PostgreSQL cursor object used for executing SQL commands.
        data_df (pd.DataFrame): Pandas DataFrame containing the data to be inserted.
        schema (str): The database schema where the target table is located (not used in this implementation).
        table_name (str): The name of the table to insert data into (not used in this implementation).
        """

        data_values: list[tuple] = [tuple(a) for a in data_df.values]

        placeholders = ", ".join(["%s"] * len(data_values[0]))
        full_table_name = f"{schema.upper()}.{table_name.upper()}"
        sql_insert_to_execute = f"INSERT INTO {full_table_name} VALUES ({placeholders})"

        cur.executemany(sql_insert_to_execute, data_values)
