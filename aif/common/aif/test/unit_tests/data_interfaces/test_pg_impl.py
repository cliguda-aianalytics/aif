"""Unit tests for the pg_impl module - The connection is mocked to simulate a successful connection
and verify the logic, not the actual database connection.

This module contains comprehensive tests for the PostgreSQL database implementation,
covering connection management, SQL statement generation, and batch insert operations.
"""

from unittest.mock import Mock, patch
import numpy as np
import pandas as pd
import pytest
import psycopg

from aif.common.aif.src.data_interfaces.pg_impl import PGImpl


class TestPGImpl:
    """Test suite for the PGImpl class."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.pg_impl = PGImpl()  # pylint: disable=attribute-defined-outside-init
        self.valid_db_settings = {  # pylint: disable=attribute-defined-outside-init
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            "port": "5432",
            "db_name": "test_db",
        }

    def test_get_connection_success(self):
        """Test successful database connection."""
        with patch("psycopg.connect") as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            result = self.pg_impl.get_connection(self.valid_db_settings)

            expected_connection_str = "postgresql://test_user:test_password@localhost:5432/test_db"
            mock_connect.assert_called_once_with(expected_connection_str, autocommit=False)
            assert result == mock_connection

    def test_get_connection_with_spaces_in_values(self):
        """Test connection string generation with spaces in values (they should be removed)."""
        db_settings_with_spaces = {
            "user": "test user",
            "password": "test password",
            "host": "local host",
            "port": "5432",
            "db_name": "test db",
        }

        with patch("psycopg.connect") as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            result = self.pg_impl.get_connection(db_settings_with_spaces)

            expected_connection_str = "postgresql://testuser:testpassword@localhost:5432/testdb"
            mock_connect.assert_called_once_with(expected_connection_str, autocommit=False)
            assert result == mock_connection

    def test_get_connection_missing_required_field(self):
        """Test connection with missing required database settings."""
        incomplete_settings = {
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            # Missing port and db_name
        }

        with patch("psycopg.connect"):
            with pytest.raises(KeyError):
                self.pg_impl.get_connection(incomplete_settings)

    def test_get_connection_psycopg_error(self):
        """Test connection when psycopg raises an exception."""
        with patch("psycopg.connect") as mock_connect:
            mock_connect.side_effect = psycopg.OperationalError("Connection failed")

            with pytest.raises(psycopg.OperationalError):
                self.pg_impl.get_connection(self.valid_db_settings)

    def test_get_refresh_materialized_view_stmt(self):
        """Test generation of refresh materialized view statement."""
        view_name = "test_view"
        expected_stmt = "REFRESH MATERIALIZED VIEW test_view"

        result = self.pg_impl.get_refresh_materialized_view_stmt(view_name)

        assert result == expected_stmt

    def test_get_refresh_materialized_view_stmt_with_schema(self):
        """Test refresh materialized view statement with schema-qualified view name."""
        view_name = "schema.test_view"
        expected_stmt = "REFRESH MATERIALIZED VIEW schema.test_view"

        result = self.pg_impl.get_refresh_materialized_view_stmt(view_name)

        assert result == expected_stmt

    def test_get_parameter_placeholder(self):
        """Test parameter placeholder generation for various indices."""
        test_cases = [(1, "$1"), (2, "$2"), (10, "$10"), (100, "$100")]

        for param_index, expected_placeholder in test_cases:
            result = self.pg_impl.get_parameter_placeholder(param_index)
            assert result == expected_placeholder

    def test_wrap_sql_stmt(self):
        """Test SQL statement wrapping (PostgreSQL doesn't require wrapping)."""
        test_statements = [
            "SELECT * FROM table1;",
            "INSERT INTO table1 VALUES (1, 'test');",
            "CREATE TABLE test (id INT);",
            "SELECT * FROM table1; INSERT INTO table2 VALUES (1);",
        ]

        for sql_stmt in test_statements:
            result = self.pg_impl.wrap_sql_stmt(sql_stmt)
            assert result == sql_stmt

    def test_execute_batch_insert_stmt_success(self):
        """Test successful batch insert with custom SQL statement."""
        mock_cursor = Mock()
        sql_stmt = "INSERT INTO test_table VALUES ($1, $2) ON CONFLICT DO NOTHING"
        test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert_stmt(mock_cursor, sql_stmt, test_data, schema, table_name)

        expected_data_values = [(1, "a"), (2, "b"), (3, "c")]
        mock_cursor.executemany.assert_called_once_with(sql_stmt, expected_data_values)

    def test_execute_batch_insert_stmt_empty_dataframe(self):
        """Test batch insert with empty DataFrame."""
        mock_cursor = Mock()
        sql_stmt = "INSERT INTO test_table VALUES ($1, $2)"
        empty_df = pd.DataFrame()
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert_stmt(mock_cursor, sql_stmt, empty_df, schema, table_name)

        mock_cursor.executemany.assert_called_once_with(sql_stmt, [])

    def test_execute_batch_insert_stmt_with_mixed_types(self):
        """Test batch insert with DataFrame containing mixed data types."""
        mock_cursor = Mock()
        sql_stmt = "INSERT INTO test_table VALUES ($1, $2, $3, $4)"
        test_data = pd.DataFrame(
            {
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True],
            }
        )
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert_stmt(mock_cursor, sql_stmt, test_data, schema, table_name)

        expected_data_values = [(1, 1.1, "a", True), (2, 2.2, "b", False), (3, 3.3, "c", True)]
        mock_cursor.executemany.assert_called_once_with(sql_stmt, expected_data_values)

    def test_execute_batch_insert_stmt_with_null_values(self):
        """Test batch insert with DataFrame containing null values."""

        mock_cursor = Mock()
        sql_stmt = "INSERT INTO test_table VALUES ($1, $2)"
        test_data = pd.DataFrame({"col1": [1, None, 3], "col2": ["a", "b", None]})
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert_stmt(mock_cursor, sql_stmt, test_data, schema, table_name)

        # Verify the call was made
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args[0]

        # Check the SQL statement
        assert call_args[0] == sql_stmt

        # Check the data values (with special handling for NaN)
        actual_data_values = call_args[1]
        assert len(actual_data_values) == 3

        # Check first row
        assert actual_data_values[0] == (1.0, "a")

        # Check second row (with NaN handling)
        assert np.isnan(actual_data_values[1][0])
        assert actual_data_values[1][1] == "b"

        # Check third row
        assert actual_data_values[2] == (3.0, None)

    def test_execute_batch_insert_success(self):
        """Test successful batch insert with generated SQL statement."""
        mock_cursor = Mock()
        test_data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert(mock_cursor, test_data, schema, table_name)

        expected_sql = "INSERT INTO TEST_SCHEMA.TEST_TABLE VALUES (%s, %s)"
        expected_data_values = [(1, "a"), (2, "b"), (3, "c")]
        mock_cursor.executemany.assert_called_once_with(expected_sql, expected_data_values)

    def test_execute_batch_insert_empty_dataframe(self):
        """Test batch insert with empty DataFrame raises IndexError."""
        mock_cursor = Mock()
        empty_df = pd.DataFrame()
        schema = "test_schema"
        table_name = "test_table"

        # Current implementation doesn't handle empty DataFrames gracefully
        # It will raise IndexError when trying to access data_values[0]
        with pytest.raises(IndexError):
            self.pg_impl.execute_batch_insert(mock_cursor, empty_df, schema, table_name)

    def test_execute_batch_insert_single_column(self):
        """Test batch insert with single column DataFrame."""
        mock_cursor = Mock()
        test_data = pd.DataFrame({"col1": [1, 2, 3]})
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert(mock_cursor, test_data, schema, table_name)

        expected_sql = "INSERT INTO TEST_SCHEMA.TEST_TABLE VALUES (%s)"
        expected_data_values = [(1,), (2,), (3,)]
        mock_cursor.executemany.assert_called_once_with(expected_sql, expected_data_values)

    def test_execute_batch_insert_case_sensitivity(self):
        """Test that schema and table names are properly uppercased."""
        mock_cursor = Mock()
        test_data = pd.DataFrame({"col1": [1, 2]})
        schema = "lowercase_schema"
        table_name = "lowercase_table"

        self.pg_impl.execute_batch_insert(mock_cursor, test_data, schema, table_name)

        expected_sql = "INSERT INTO LOWERCASE_SCHEMA.LOWERCASE_TABLE VALUES (%s)"
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args[0]
        assert call_args[0] == expected_sql

    def test_execute_batch_insert_with_special_characters(self):
        """Test batch insert with DataFrame containing special characters."""
        mock_cursor = Mock()
        test_data = pd.DataFrame(
            {"col1": ["test with spaces", "test'with'quotes", "test\nwith\nnewlines"], "col2": [1, 2, 3]}
        )
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert(mock_cursor, test_data, schema, table_name)

        expected_data_values = [("test with spaces", 1), ("test'with'quotes", 2), ("test\nwith\nnewlines", 3)]
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args[0]
        assert call_args[1] == expected_data_values

    def test_execute_batch_insert_with_datetime(self):
        """Test batch insert with DataFrame containing datetime values."""
        mock_cursor = Mock()
        test_data = pd.DataFrame(
            {"col1": [1, 2, 3], "date_col": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])}
        )
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert(mock_cursor, test_data, schema, table_name)

        # The datetime values should be converted to tuples as-is
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args[0]
        assert len(call_args[1]) == 3  # Should have 3 rows
        assert len(call_args[1][0]) == 2  # Should have 2 columns per row


class TestPGImplEdgeCases:
    """Test suite for edge cases and error conditions in PGImpl."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        self.pg_impl = PGImpl()  # pylint: disable=attribute-defined-outside-init

    def test_get_parameter_placeholder_zero_index(self):
        """Test parameter placeholder with zero index."""
        result = self.pg_impl.get_parameter_placeholder(0)
        assert result == "$0"

    def test_get_parameter_placeholder_negative_index(self):
        """Test parameter placeholder with negative index."""
        result = self.pg_impl.get_parameter_placeholder(-1)
        assert result == "$-1"

    def test_get_refresh_materialized_view_stmt_empty_string(self):
        """Test refresh materialized view statement with empty view name."""
        result = self.pg_impl.get_refresh_materialized_view_stmt("")
        assert result == "REFRESH MATERIALIZED VIEW "

    def test_wrap_sql_stmt_empty_string(self):
        """Test wrapping empty SQL statement."""
        result = self.pg_impl.wrap_sql_stmt("")
        assert result == ""

    def test_wrap_sql_stmt_simple_statement(self):
        """Test wrapping a simple SQL statement."""
        test_sql = "SELECT * FROM test_table"
        result = self.pg_impl.wrap_sql_stmt(test_sql)
        assert result == test_sql

    def test_execute_batch_insert_with_large_dataframe(self):
        """Test batch insert with large DataFrame to ensure performance."""
        mock_cursor = Mock()

        # Create a large DataFrame
        large_data = pd.DataFrame({"col1": range(10000), "col2": [f"value_{i}" for i in range(10000)]})
        schema = "test_schema"
        table_name = "test_table"

        self.pg_impl.execute_batch_insert(mock_cursor, large_data, schema, table_name)

        # Verify that executemany was called with the correct number of rows
        mock_cursor.executemany.assert_called_once()
        call_args = mock_cursor.executemany.call_args[0]
        assert len(call_args[1]) == 10000
