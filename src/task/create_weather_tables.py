"""天氣觀測事實表的 DDL 宣告與建表函式。

`WEATHER_TABLES` 以「資料表名稱對應 CREATE TABLE 敘述」的形式定義
`fact_hourly_weather`，一列是一個觀測點在某個整點的天氣。經緯度存的是已進位到
氣象網格的座標，並各自建索引，供事故資料依時間與位置比對天氣。
唯一鍵是 `hash_value`，由觀測時間與座標湊出，確保重跑不會產生重複列。
字典的鍵必須與DDL 實際建立的資料表同名，`create_tables()` 的存在性檢查才會正確。
"""

from sqlalchemy import Engine

from src.util.mysql_utils import create_tables

# key 必須與 DDL 實際建立的表同名，因為 create_tables() 會以它在內部做 IF EXISTS 檢查。
WEATHER_TABLES = {
    "fact_hourly_weather": """CREATE TABLE IF NOT EXISTS `fact_hourly_weather`(
                        `weather_record_id` BIGINT AUTO_INCREMENT COMMENT '天氣觀測紀錄編號',
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
    """建立天氣觀測事實表 `fact_hourly_weather`，已存在則略過。

    Args:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。

    Raises:
        SQLAlchemyError: DDL 執行失敗，事務復原後往外拋。
    """
    create_tables(engine, WEATHER_TABLES)
