"""建立天氣觀測事實表的 DDL 宣告。"""

from sqlalchemy import Engine

from src.util.mysql_utils import create_tables

# 表名 -> CREATE TABLE 敘述。鍵必須與 DDL 實際建立的表同名，
# create_tables() 的存在性檢查才會正確。
WEATHER_TABLES = {
    "fact_hourly_weather": """CREATE TABLE IF NOT EXISTS `fact_hourly_weather`(
                        `weather_record_id` BIGINT AUTO_INCREMENT,
                        `observation_datetime` DATETIME NOT NULL COMMENT '觀測日期時間(yyyy/mm/dd_HH:MM)',
                        `temperature_degree` DECIMAL(6,2) COMMENT '氣溫(℃)',
                        `apparent_temperature_degree` DECIMAL(6,2) COMMENT '體感溫度(℃)',
                        `rain_within_hour_mm` DECIMAL(6,2) COMMENT '前一小時內降雨量(mm)',
                        `precipitation_mm` DECIMAL(6,2) COMMENT '前一小時內降雨降雪量(mm)',
                        `weather_code` INT COMMENT '天氣代碼WMO code',
                        `wind_speed_10m_km_per_h` DECIMAL(6,2) COMMENT '風速(km_per_hour)',
                        `wind_gusts_10m_km_per_h` DECIMAL(6,2) COMMENT '最大陣風(km_per_hour)',
                        `longitude_round` DECIMAL(6,2) COMMENT '觀測點經度(已進位到氣象網格)',
                        `latitude_round` DECIMAL(6,2) COMMENT '觀測點緯度(已進位到氣象網格)',
                        `hash_value` CHAR(32) NOT NULL,
                        PRIMARY KEY (`weather_record_id`),
                        UNIQUE KEY UK_WHH_hash (`hash_value`),
                        INDEX idx_fact_hourly_weather_obt(`observation_datetime`),
                        INDEX idx_fact_hourly_weather_long(`longitude_round`),
                        INDEX idx_fact_hourly_weather_lat(`latitude_round`)
                        ) CHARSET=utf8mb4 COMMENT '各地天氣觀測結果';
                        """,
}


def create_weather_tables(engine: Engine) -> None:
    """建立天氣觀測事實表 `fact_hourly_weather`（已存在則略過）。

    Parameters:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。
    """
    create_tables(engine, WEATHER_TABLES)
