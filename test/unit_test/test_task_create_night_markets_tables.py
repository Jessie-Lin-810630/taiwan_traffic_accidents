"""Unit tests for src.task.create_night_markets_tables module."""

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("redis", MagicMock())

from src.task.create_night_markets_tables import create_night_market_tables


class TestCreateNightMarketTables:
    """Test suite for create_night_market_tables function."""

    def test_create_table_executes_ddl_and_disposes_engine(self):
        """It should execute the DDL and always dispose the engine."""
        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_context

        result = create_night_market_tables(mock_engine)

        assert result is None
        mock_engine.connect.assert_called_once()
        mock_conn.execute.assert_called_once()
        executed_sql = str(mock_conn.execute.call_args.args[0])
        assert "CREATE TABLE IF NOT EXISTS `fact_night_markets`" in executed_sql
        mock_engine.dispose.assert_called_once()

    def test_create_table_handles_database_error_and_still_disposes(self):
        """It should swallow execution errors and dispose the engine."""
        mock_context = MagicMock()
        mock_context.__enter__.side_effect = Exception("boom")

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_context

        result = create_night_market_tables(mock_engine)

        assert result is None
        mock_engine.dispose.assert_called_once()
