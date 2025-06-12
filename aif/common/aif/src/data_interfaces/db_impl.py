"""
Database implementation interface module.

Different databases need different implementations and this module provides an abstract base class for
them. This module defines the DBImpl abstract base class which serves as an interface for various
database implementations (PostgreSQL, MySQL, etc.) to ensure consistent interaction patterns across
different database systems.
"""

from abc import ABC, abstractmethod

import pandas as pd


class DBImpl(ABC):
    """
    Abstract Base Class for database implementations.

    This class defines the interface that all specific database implementations must adhere to.
    Concrete subclasses should implement all abstract methods to provide database-specific
    functionality for connection management, query execution, and other database operations.
    """

    @abstractmethod
    def get_connection(self, db_settings):
        """
        Establish and return a connection to the database.

        Args:
            db_settings: Configuration parameters required to connect to the database.
                         The exact structure depends on the specific database implementation.

        Returns:
            A database connection object specific to the database implementation.
        """

    @abstractmethod
    def get_refresh_materialized_view_stmt(self, view_name: str) -> str:
        """
        Return the SQL statement to refresh a materialized view.

        Args:
            view_name: The name of the materialized view to refresh.

        Returns:
            str: SQL statement that can be executed to refresh the specified materialized view.
        """

    @abstractmethod
    def get_parameter_placeholder(self, param_index: int) -> str:
        """
        Return the parameter placeholder for the specific database.

        Different databases use different syntax for parameter placeholders in prepared statements.
        For example, PostgreSQL uses $1, $2, etc., while SQLite uses ?, and others use %s.

        Args:
            param_index: The index of the parameter in the query.

        Returns:
            str: The database-specific placeholder string for the given parameter index.
        """

    @abstractmethod
    def wrap_sql_stmt(self, sql_stmt: str) -> str:
        """
        Wrap SQL statements according to database-specific requirements.

        For some databases, multiple statements must be wrapped with BEGIN END,
        while others don't allow that or require different syntax.

        Args:
            sql_stmt: The SQL statement(s) to be wrapped.

        Returns:
            str: The properly wrapped SQL statement(s) for the specific database.
        """

    @abstractmethod
    def execute_batch_insert_stmt(
        self, cur, sql_stmt: str, data_df: pd.DataFrame, schema: str, table_name: str
    ) -> None:
        """
        Execute a batch insert operation with the appropriate logic and SQL statement for the database.

        This method uses a given SQL statement to insert multiple rows from a DataFrame
        into a database table using database-specific optimizations.
        (Useful, when a custom statement is needed on how to handle constrains)

        Args:
            cur: Database cursor object used for executing SQL commands.
            sql_stmt: The SQL insert statement template.
            data_df: Pandas DataFrame containing the data to be inserted.
            schema: The database schema where the target table is located.
            table_name: The name of the table to insert data into.

        Returns:
            None
        """

    @abstractmethod
    def execute_batch_insert(self, cur, data_df: pd.DataFrame, schema: str, table_name: str) -> None:
        """
        Execute a batch insert operation with the appropriate syntax for the database.

        This method uses plain insert fand is useful, if no constrains or special cases need to be handled.

        Args:
            cur: Database cursor object used for executing SQL commands.
            data_df: Pandas DataFrame containing the data to be inserted.
            schema: The database schema where the target table is located.
            table_name: The name of the table to insert data into.

        Returns:
            None
        """
