"""Unit tests for src.task.create_traffic_accident_tables module."""

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("redis", MagicMock())

from src.task.create_traffic_accident_tables import create_traffic_accident_tables


class TestCreateTrafficAccidentTables:
    """Test suite for create_traffic_accident_tables function."""

    def test_create_tables_executes_all_ddls(self):
        """It should execute the expected table creation statements."""
        mock_conn = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_context

        create_traffic_accident_tables(mock_engine)

        assert mock_conn.execute.call_count == 7
        executed_sql = [str(call.args[0]) for call in mock_conn.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS `dim_accident_day`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS `dim_road_design`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS `dim_lane_design`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS `dim_accident_type`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS `fact_accident_main`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS`fact_accident_env`" in sql for sql in executed_sql)
        assert any("CREATE TABLE IF NOT EXISTS `fact_accident_human`" in sql for sql in executed_sql)
        mock_engine.dispose.assert_called_once()

    def test_create_tables_handles_error_and_disposes_engine(self):
        """It should dispose the engine even when execution fails."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("ddl failure")

        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_conn

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_context

        create_traffic_accident_tables(mock_engine)

        mock_engine.dispose.assert_called_once()
