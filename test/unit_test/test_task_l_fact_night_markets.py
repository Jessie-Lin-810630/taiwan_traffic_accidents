"""Unit tests for src.task.l_fact_night_markets module."""

import sys
from unittest.mock import MagicMock, patch

import pandas as pd

sys.modules.setdefault("redis", MagicMock())

from src.task.l_fact_night_markets import l_fact_night_markets


class TestLFactNightMarkets:
    """Test suite for l_fact_night_markets function."""

    def test_successful_insert_adds_updated_on_and_commits(self):
        """It should insert rows and commit the transaction."""
        test_df = pd.DataFrame({
            "nightmarket_name": ["士林夜市"],
            "business_days_weekday": ["星期一"],
            "business_hours_opening": ["16:00:00"],
            "business_hours_closing": ["23:59:59"],
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_night_markets.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_night_markets(test_df, "test_db")

        mock_cursor.executemany.assert_called_once()
        sql_statement, rows = mock_cursor.executemany.call_args.args
        assert "INSERT INTO fact_night_markets" in sql_statement
        assert "updated_on=VALUES(updated_on)" in sql_statement
        assert len(rows) == 1
        assert len(rows[0]) == len(test_df.columns)
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_database_error_rolls_back_transaction(self):
        """It should roll back when insert execution fails."""
        test_df = pd.DataFrame({
            "nightmarket_name": ["士林夜市"],
            "business_days_weekday": ["星期一"],
        })

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("insert failed")
        mock_conn.cursor.return_value = mock_cursor

        with patch("src.task.l_fact_night_markets.get_pymysql_conn_to_mysql", return_value=mock_conn):
            l_fact_night_markets(test_df, "test_db")

        mock_conn.rollback.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    def test_none_connection_skips_cursor_work(self):
        """It should skip execution if no connection is returned."""
        test_df = pd.DataFrame({
            "nightmarket_name": ["士林夜市"],
            "business_days_weekday": ["星期一"],
        })

        with patch("src.task.l_fact_night_markets.get_pymysql_conn_to_mysql", return_value=None):
            result = l_fact_night_markets(test_df, "test_db")

        assert result is None
