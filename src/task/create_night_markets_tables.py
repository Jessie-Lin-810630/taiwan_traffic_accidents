"""夜市事實表的 DDL 宣告與建表函式。

`NIGHT_MARKET_TABLES` 以「資料表名稱對應 CREATE TABLE 敘述」的形式定義
`fact_night_markets`，內容為全臺夜市的地理位置、營業時間與 Google 地圖資訊。
唯一鍵是「緯度 + 經度 + 營業星期」的組合，因此同一個夜市的不同營業日各佔一列。
字典的鍵必須與 DDL 實際建立的資料表同名，`create_tables()` 的存在性檢查才會正確。
"""

from sqlalchemy import Engine

from src.util.mysql_utils import create_tables

# key 必須與 DDL 實際建立的表同名，因為 create_tables() 會以它在內部做 IF EXISTS 檢查。
NIGHT_MARKET_TABLES = {
    "fact_night_markets": """CREATE TABLE IF NOT EXISTS `fact_night_markets`(
                        `nightmarket_id` INT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '夜市代碼',
                        `nightmarket_name` VARCHAR(30) COMMENT '夜市名稱',
                        `region` VARCHAR(10) COMMENT '夜市所屬地區(北、中、南部)',
                        `zipcode` VARCHAR(10) COMMENT '夜市所屬郵遞區號',
                        `city` VARCHAR(10) COMMENT '夜市所屬第一、第二行政區(只呈現: xx市/xx縣)',
                        `district` VARCHAR(10) COMMENT '夜市所屬第二、三行政區(只呈現：xx區)',
                        `area_road` VARCHAR(50) COMMENT '夜市所在街道地址',
                        `latitude` DECIMAL(10,6) COMMENT '夜市中心緯度',
                        `longitude` DECIMAL(10,6) COMMENT '夜市中心經度',
                        `googlemap_rating` FLOAT COMMENT 'GoogleMap評論星度',
                        `business_hours_opening` TIME COMMENT '當日開始營業時間',
                        `business_hours_closing` TIME COMMENT '當日結束營業時間',
                        `business_days_weekday` VARCHAR(10) COMMENT '星期一～日',
                        `url_to_googlemap` VARCHAR(200) COMMENT 'url to GoogleMap',
                        `northeast_latitude` DECIMAL(10,6) COMMENT '夜市東南端點緯度',
                        `northeast_longitude` DECIMAL(10,6) COMMENT '夜市東南端點經度',
                        `southwest_latitude` DECIMAL(10,6) COMMENT '夜市西南端緯度',
                        `southwest_longitude` DECIMAL(10,6) COMMENT '夜市西南端經度',
                        `created_on` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '資料列插入時間日期',
                        `updated_on` TIMESTAMP COMMENT '更新時間日期',
                        CONSTRAINT `uk_nm_latlonwkd` UNIQUE(`latitude`,
                                                            `longitude`,
                                                            `business_days_weekday`)
                        ) CHARSET=utf8mb4 COMMENT '全臺灣夜市地理資訊與營業時間表';
                        """,
}


def create_night_market_tables(engine: Engine) -> None:
    """建立夜市事實表 `fact_night_markets`，已存在則略過。

    Args:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。

    Raises:
        SQLAlchemyError: DDL 執行失敗，事務復原後往外拋。
    """
    create_tables(engine, NIGHT_MARKET_TABLES)
