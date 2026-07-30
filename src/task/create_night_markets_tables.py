"""建立夜市事實表的 DDL 任務。"""

from sqlalchemy import Engine, text

from src.util.logger_crtx import get_logger

logger = get_logger(__name__)


def create_night_market_tables(engine: Engine) -> None:
    """建立夜市事實表 `fact_night_markets`（已存在則略過）。

    Parameters:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。
    """
    try:
        # engine.begin() 會在離開 context 時自動提交，失敗則自動 rollback；
        # engine.connect() 預設不提交，靠 MySQL 對 DDL 的隱式提交會生效，但不應該仰賴這種隱式提交。
        with engine.begin() as conn:
            logger.info("Creating table 'fact_night_markets'...")
            ddl_str = """CREATE TABLE IF NOT EXISTS `fact_night_markets`(
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
                        """
            conn.execute(text(ddl_str))
            logger.info("Table 'fact_night_markets' created successfully.")
    except Exception:
        logger.error("An error occurred while creating the table.", exc_info=True)
        raise
    finally:
        engine.dispose()
    return None
