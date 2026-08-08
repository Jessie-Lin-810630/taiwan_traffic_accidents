"""交通事故星狀綱要的 DDL 宣告與建表函式。

`TRAFFIC_ACCIDENT_TABLES` 以「資料表名稱對應 CREATE TABLE 敘述」的形式集中
4 張維度表（`dim_accident_day`、`dim_road_design`、`dim_lane_design`、
`dim_accident_type`）與 3 張事實表（`fact_accident_main`、`fact_accident_env`、
`fact_accident_human`）的定義。字典的鍵必須與 DDL 實際建立的資料表同名，
`create_tables()` 的存在性檢查才會正確。

維度表一律以業務欄位的組合作為唯一鍵、另設自增的代理鍵供 JOIN 使用；
建表順序即字典的順序，被外鍵參考的表都排在參考它的表之前。
"""

from sqlalchemy import Engine

from src.util.mysql_utils import create_tables

# key 必須與 DDL 實際建立的表同名，因為 create_tables() 會以它在內部做 IF EXISTS 檢查。
# 不可隨便調換字典序，因為後續建表順序按照字典序，而被外鍵參考的表都排在參考它的表之前。
TRAFFIC_ACCIDENT_TABLES = {
    # 預計5年只會有幾千筆日期，因此以 accident_date 作為唯一鍵確保業務邏輯不重複，day_id 則作為 surrogate key 方便 JOIN
    "dim_accident_day": """CREATE TABLE IF NOT EXISTS `dim_accident_day` (
                                    `day_id` INT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '日編號ID',
                                    `accident_date` DATE COMMENT '車禍日期',
                                    `accident_weekday` VARCHAR(10) COMMENT '車禍發生星期',
                                    `is_holiday` TINYINT COMMENT '是否放假',
                                    `national_activity` VARCHAR(20) COMMENT '是否有全國性活動，例如：總統上任、公投日、國定假日',
                                    CONSTRAINT `uk_dim_accidentday_date` UNIQUE (`accident_date`)
                                    ) charset=utf8mb4 COMMENT '交通意外日維度表';
                            """,
    "dim_road_design": """CREATE TABLE IF NOT EXISTS `dim_road_design` (
                                    `road_design_id` BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '道路設計編號ID',
                                    `road_type_primary_party` VARCHAR(10) COMMENT '行駛路線之道路類別，對應原資料集''道路類別''',
                                    `road_form_major` VARCHAR(10) COMMENT '道路類別之道路型態，對應原資料集''道路型態大類別名稱''',
                                    `road_form_minor` VARCHAR(10) COMMENT '道路類別之道路細項，對應原資料集''道路型態子類別名稱''',
                                    CONSTRAINT `uk_dim_roaddesign_roadtypeandform` UNIQUE (`road_type_primary_party`, `road_form_major`, `road_form_minor`)
                                    ) charset=utf8mb4 COMMENT '道路設計維度表';
                            """,
    # 預計5年只會有700種車道設計，因此以車道設計的各個屬性作為唯一鍵確保業務邏輯不重複，lane_design_id 則作為 surrogate key 方便 JOIN
    "dim_lane_design": """CREATE TABLE IF NOT EXISTS `dim_lane_design` (
                                    `lane_design_id` BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '車道設計編號ID',
                                    `lane_divider_direction_major` VARCHAR(20) COMMENT '車道分向設施大類別名稱',
                                    `lane_divider_direction_minor` VARCHAR(20) COMMENT '車道分向設施子類別名稱',
                                    `lane_divider_main_general` VARCHAR(20) COMMENT '車道分道設施-快車道或一般車道間名稱''',
                                    `lane_divider_fast_slow` VARCHAR(20) COMMENT '車道分道設施-快慢車道間名稱',
                                    `lane_edge_marking` VARCHAR(2) COMMENT '是否有路面邊線，對應原資料集''車道劃分設施-分道設施-路面邊線名稱''',
                                    CONSTRAINT `uk_dim_lanedesign_dividerandedge` UNIQUE (`lane_divider_direction_major`,
                                                                                          `lane_divider_direction_minor`,
                                                                                          `lane_divider_main_general`,
                                                                                          `lane_divider_fast_slow`,
                                                                                          `lane_edge_marking`)
                                    ) charset=utf8mb4 COMMENT '車道設計維度表';
                            """,
    # 預計5年只會有幾千種事故類別，因此以事故類別的各個屬性作為唯一鍵確保業務邏輯不重複，accident_type_id 則作為 surrogate key 方便 JOIN
    "dim_accident_type": """CREATE TABLE IF NOT EXISTS `dim_accident_type` (
                                    `accident_type_id` BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '事故類別編號ID',
                                    `accident_category` VARCHAR(2) COMMENT '車禍級別(A1、A2)',
                                    `accident_position_major` VARCHAR(20) COMMENT '事故位置大類別名稱',
                                    `accident_position_minor` VARCHAR(20) COMMENT '事故位置子類別名稱',
                                    `accident_type_major` VARCHAR(20) COMMENT '簡述此事故涉及的實體，例如：人車、兩車或一台車本身，對應原資料集-事故類型及型態大類別名稱',
                                    `accident_type_minor` VARCHAR(20) COMMENT '簡述此事故涉及的實體的相互作用，例如：追撞、側撞、翻車、衝出路外，對應原資料集-事故類型及型態大類別名稱',
                                    CONSTRAINT `uk_dim_accidenttype_categoryandposition` UNIQUE (`accident_category`, `accident_position_major`,
                                                                                                 `accident_position_minor`, `accident_type_major`,
                                                                                                 `accident_type_minor`)
                                ) charset=utf8mb4 COMMENT '事故類別維度表';
                            """,
    # 使用 accident_id 作為主鍵，並以 day_id、accident_time、longitude、latitude 的組合作為唯一鍵確保業務邏輯不重複，並在經緯度上建立索引以加速地理空間查詢
    "fact_accident_main": """CREATE TABLE IF NOT EXISTS `fact_accident_main` (
                                    `accident_id` VARCHAR(16) PRIMARY KEY NOT NULL COMMENT '車禍案件編號',
                                    `accident_type_id` BIGINT NOT NULL COMMENT '事故類別編號ID',
                                    `day_id` INT NOT NULL COMMENT '日編號ID',
                                    `weather_record_id` BIGINT NOT NULL DEFAULT -1 COMMENT '天氣觀測紀錄編號',
                                    `accident_time` time COMMENT '車禍時段(HH:MM:SS)',
                                    `death_count` INT COMMENT '死亡人數',
                                    `injury_count` INT COMMENT '受傷人數',
                                    `longitude` decimal(10,6) COMMENT '經度',
                                    `latitude` decimal(10,6) COMMENT '緯度',
                                    CONSTRAINT `fk_fact_accmain_accidenttypeid` FOREIGN KEY (`accident_type_id`)
                                        REFERENCES `dim_accident_type`(`accident_type_id`),
                                    CONSTRAINT `fk_fact_accmain_dayid` FOREIGN KEY (`day_id`)
                                        REFERENCES `dim_accident_day`(`day_id`),
                                    UNIQUE KEY `uk_fact_accmain_daytimelonlat` (`day_id`, `accident_time`,
                                                                                `longitude`,`latitude`),
                                    INDEX `idx_fact_accmain_lon` (`longitude`),
                                    INDEX `idx_fact_accmain_lat` (`latitude`)
                                    ) CHARSET=utf8mb4 COMMENT='車禍案件事實表';
                            """,
    # main 與 env 兩張事實表互為 one-to-one，因此子表的 PRIMARY KEY 完全可以同時作為 FOREIGN KEY，參考主表的 PRIMARY KEY
    "fact_accident_env": """CREATE TABLE IF NOT EXISTS`fact_accident_env` (
                                    `accident_id` VARCHAR(16) NOT NULL COMMENT '車禍案件編號',
                                    `weather_condition` VARCHAR(10) COMMENT '天氣簡述',
                                    `light_condition` VARCHAR(20) COMMENT '行駛路線上燈光照明狀態',
                                    `speed_limit_primary_party` SMALLINT COMMENT '行駛路線之當下速限，對應原資料集''速限''',
                                    `road_design_id` BIGINT NOT NULL COMMENT '道路設計編號ID',
                                    `lane_design_id` BIGINT NOT NULL COMMENT '車道設計編號ID',
                                    `road_surface_pavement` VARCHAR(10) COMMENT '路面鋪裝材質',
                                    `road_surface_condition` VARCHAR(10) COMMENT '路面濕滑狀態',
                                    `road_surface_defect` VARCHAR(10) COMMENT '路面是否缺陷',
                                    `road_obstacle` VARCHAR(10) COMMENT '路面是否有障礙物',
                                    `sight_distance_quality` VARCHAR(10) COMMENT '視距是否有遮蔽物，對應原資料集''道路障礙-視距品質名稱''',
                                    `sight_distance` VARCHAR(10) COMMENT '視距是否足夠遠，對應原資料集''道路障礙-視距名稱''',
                                    `traffic_signal_type` VARCHAR(50) COMMENT '行駛路線上號誌種類，對應原資料集''號誌-號誌種類名稱''',
                                    `traffic_signal_action` VARCHAR(10) COMMENT '行駛路線上號誌運作狀態，對應原資料集''號誌-號誌動作名稱''',
                                    PRIMARY KEY (`accident_id`),
                                    CONSTRAINT `fk_fact_accidentenv_accidentid` FOREIGN KEY (`accident_id`)
                                        REFERENCES `fact_accident_main`(`accident_id`),
                                    CONSTRAINT `fk_fact_accidentenv_roaddesignid` FOREIGN KEY (`road_design_id`)
                                        REFERENCES `dim_road_design`(`road_design_id`),
                                    CONSTRAINT `fk_fact_accidentenv_lanedesignid` FOREIGN KEY (`lane_design_id`)
                                        REFERENCES `dim_lane_design`(`lane_design_id`)
                                    ) CHARSET=utf8mb4 COMMENT='車禍案件環境事實表';
                            """,
    # row_hash 在 T 階段用 accident_id、party_sequence、age、gender、impact_point_minor_other 湊出
    "fact_accident_human": """CREATE TABLE IF NOT EXISTS `fact_accident_human` (
                                    `person_id` BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL COMMENT '涉案人ID',
                                    `accident_id` VARCHAR(16) NOT NULL COMMENT '車禍案件編號',
                                    `party_sequence` INT COMMENT '肇事責任順位',
                                    `is_primary_party_sequence` TINYINT COMMENT '是否為第一肇事者',
                                    `gender` VARCHAR(20) COMMENT '性別',
                                    `age` SMALLINT COMMENT '年齡',
                                    `protective_equipment` VARCHAR(50) COMMENT '事發時所穿戴保護裝',
                                    `mobile_device_usage` VARCHAR(20) COMMENT '事發時是否穿戴行動裝置',
                                    `party_action_major` VARCHAR(20) COMMENT '事發時處於駕車或未駕車狀態，對應原資料集''當事者行動狀態大類別名稱''',
                                    `party_action_minor` VARCHAR(20) COMMENT '承major，補述狀態動作細節，對應原資料集''當事者行動狀態子類別名稱''',
                                    `vehicle_type_major` VARCHAR(20) COMMENT '駕駛車種類別，對應原資料集''當事者區分-類別-大類別名稱-車種''',
                                    `vehicle_type_minor` VARCHAR(20) COMMENT '駕駛車種補述，對應原資料集''當事者區分-類別-子類別名稱-車種''',
                                    `cause_analysis_major_individual` VARCHAR(20) COMMENT '針對當事者行為之肇因研判結果的分類，對應原資料集''肇因研判大類別名稱-個別''',
                                    `cause_analysis_minor_individual` VARCHAR(100) COMMENT '針對當事者行為之肇因研判結果的補述，對應原資料集''肇因研判子類別名稱-個別''',
                                    `serving_sharing_economy_or_delivery` VARCHAR(200) DEFAULT NULL COMMENT '當事者是否正從事共享經濟或送貨服務，對應原資料集''共享經濟或外送平台的名稱''',
                                    `impact_point_major_initial` VARCHAR(20) COMMENT '事發第一時間什麼部位撞擊到，對應原資料集''車輛撞擊部位大類別名稱-最初''',
                                    `impact_point_minor_initial` VARCHAR(20) COMMENT '事發第一時間什麼部位撞擊到之補述，對應原資料集''車輛撞擊部位子類別名稱-最初''',
                                    `impact_point_major_other` VARCHAR(20) COMMENT '事發後尚有什麼部位撞擊到，對應原資料集''車輛撞擊部位大類別名稱-其他''',
                                    `impact_point_minor_other` VARCHAR(20) COMMENT '事發後尚有什麼部位撞擊到，對應原資料集''車輛撞擊部位子類別名稱-其他''',
                                    `hit_and_run` TINYINT COMMENT '是/否肇逃',
                                    `row_hash` VARCHAR(64) UNIQUE NOT NULL COMMENT '業務邏輯雜湊值',
                                    CONSTRAINT `fk_fact_accidenthuman_accidentid` FOREIGN KEY (`accident_id`)
                                        REFERENCES `fact_accident_main`(`accident_id`)
                                    ) CHARSET=utf8mb4 COMMENT='車禍案件用路人行為事實表';
                            """,
}


def create_traffic_accident_tables(engine: Engine) -> None:
    """建立交通事故星狀綱要的 4 張維度表與 3 張事實表，已存在的表會略過。

    Args:
        engine (Engine): 已指定資料庫的 SQLAlchemy Engine。

    Raises:
        SQLAlchemyError: 任一張表的 DDL 執行失敗，事務復原後往外拋。
    """
    create_tables(engine, TRAFFIC_ACCIDENT_TABLES)
