-- 查詢2021-2026有涉及行人的事件，並串接回所有fact

-- 1. 檢視分類
# 1-1 將車禍大類別分得更乾淨，作為view表。
CREATE OR REPLACE VIEW v1_dim_accident_type AS
	(SELECT accident_type_id, 
			accident_category,
            accident_position_major,
            accident_position_minor,
            accident_type_minor,
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM dim_accident_type);
            
# 1-2 整理用路人行為分類原則，只留下跟行人有關的資料列，不管他是肇事順位多少，行人的定義不包含乘客。
CREATE OR REPLACE VIEW v2_fact_accident_human AS 
	SELECT * FROM 
		(SELECT 
			person_id, accident_id, is_primary_party_sequence,
			gender, age,
			CASE
				WHEN age < 18 AND age > 0 THEN "< 18歲"
				WHEN age < 20 AND age >= 18 THEN "18 & 19歲"
				WHEN age < 90 AND age >= 20 THEN CONCAT(ROUND(age/10)*10, " - ", 
														(ROUND(age/10)*10+9), "歲")
				ELSE "未知"
			END AS age_grouped,
			
			CASE
				WHEN age < 18 THEN "未成年"
				WHEN age >= 18 AND age < 65 THEN "青壯年者"
				WHEN age >= 65 AND age < 90 THEN "銀髮族"
				ELSE "未知"
			END AS age_cluster,
			
			protective_equipment, mobile_device_usage, party_action_major,
			party_action_minor, vehicle_type_minor,
			
			CASE
				WHEN vehicle_type_major LIKE "小客車" THEN "小客車(含客、貨兩用)"
				WHEN vehicle_type_major LIKE "小貨車" THEN "小貨車(含客、貨兩用)"
				ELSE vehicle_type_major 
			END AS vehicle_type_major_grouped,
			
			CASE
				WHEN cause_analysis_major_individual = "行人(或乘客)" THEN "非駕駛者(行人或乘客)"
				WHEN cause_analysis_major_individual = "非駕駛者" THEN "非駕駛者(行人或乘客)"
				WHEN cause_analysis_major_individual = "駕駛人" THEN "駕駛者"
				WHEN cause_analysis_major_individual = "無(車輛駕駛者因素)" THEN "駕駛者"
				ELSE cause_analysis_major_individual
			END AS cause_analysis_major_individual_grouped,
            
			cause_analysis_minor_individual,
			serving_sharing_economy_or_delivery,
			impact_point_major_initial,
			impact_point_minor_initial,
			impact_point_major_other,
			impact_point_minor_other,
			hit_and_run
				FROM fact_accident_human) tmp
                WHERE ( (vehicle_type_minor not like "乘客") AND 
						(vehicle_type_major_grouped like "人"));

-- 2. 分析時，我們需要日期時間，所以main表串day表；也需要天氣，所以main串env表；
-- 需要主要肇因，所以main串v1表；需要用路人行為，所以main串v2表。所以組了5表在一起
CREATE OR REPLACE VIEW v3_factmain_v2human AS
	(SELECT m.*,  -- 這裡的欄位需求可以自由調整
			v2.cause_analysis_minor_individual, 
            v2.cause_analysis_major_individual_grouped,
            v2.party_action_major,
            v2.vehicle_type_minor,
            v2.vehicle_type_major_grouped,
            v2.is_primary_party_sequence,
            v2.serving_sharing_economy_or_delivery
		FROM fact_accident_main m
			JOIN v2_fact_accident_human v2
            ON m.accident_id = v2.accident_id  # 先跟v2串，盡快減少資料列數，避免與其他3張表join時過久
				WHERE ((m.latitude BETWEEN 21.755 AND 25.93916)
						AND (m.longitude BETWEEN 119.30083 AND 124.56916)
					   )
	);
	

CREATE OR REPLACE VIEW v4_v3_dimday_factenv_v1 AS
	(SELECT v3.accident_id, 
			YEAR(d.accident_date) AS `accident_year`, 
            DATE_FORMAT(d.accident_date, "%Y-%m") AS `accident_yearmonth`, -- 如果不是直接在MySQL環境下互動，只需打"%Y-%m"
            d.accident_date,
            d.accident_weekday,
            d.is_holiday,
            v3.accident_time,
            HOUR(v3.accident_time) AS `accident_hourtime`,
            v3.latitude, 
            v3.longitude,
            
            CASE 
				WHEN v3.longitude > 121.5 AND v3.latitude < 24.5 THEN '東部' 
				WHEN v3.latitude > 24.45 THEN '北部'
				WHEN v3.latitude > 23.45 THEN '中部'
				ELSE '南部'
			END as region,
            
            v3.death_count, 
            v3.injury_count,
            v3.cause_analysis_minor_individual,
            v3.cause_analysis_major_individual_grouped,
            v3.party_action_major,
            v3.vehicle_type_minor,
            v3.vehicle_type_major_grouped,
            v3.is_primary_party_sequence,
            v3.serving_sharing_economy_or_delivery,
            e.weather_condition,
            e.light_condition,
            e.road_surface_condition,
            v1.accident_type_major_grouped
		FROM v3_factmain_v2human	v3
			JOIN dim_accident_day	d
            ON v3.day_id = d.day_id
				JOIN fact_accident_env	e
                ON v3.accident_id = e.accident_id
					JOIN v1_dim_accident_type	v1
					ON v3.accident_type_id = v1.accident_type_id);


-- 3. 拆成 只有肇事順位一 與 不分肇事順位 兩張表，並存成實體analysis用途表
-- 套用安全替換資料表(table swap)流程，使用RENAME完成原子操作。避免查詢服務被中斷失敗。
-- DELIMITER $$

CREATE PROCEDURE swap_analysis_table()
BEGIN
	-- 宣告變數table_exists，初始化值為0
    DECLARE table_exists INT DEFAULT 0;

    -- 建立 tmp 表
    CREATE TABLE IF NOT EXISTS analysis_pesdestrian_causing_accident_tmp
		AS (SELECT * 
				FROM v4_v3_dimday_factenv_v1 
					WHERE is_primary_party_sequence = 1);


    -- 檢查正式表(非_tmp表)是否存在，並將查詢結果寫入table_exists，如果存在，count(*)會是1
    SELECT COUNT(*) INTO table_exists
    	FROM information_schema.tables
    		WHERE table_schema = DATABASE()
      			AND table_name = "analysis_pesdestrian_causing_accident";


    -- IF/ELSE條件判斷
    IF table_exists > 0 THEN

        -- 如果存在做table swap
        RENAME TABLE 
            analysis_pesdestrian_causing_accident TO analysis_pesdestrian_causing_accident_deprecated,
            analysis_pesdestrian_causing_accident_tmp TO analysis_pesdestrian_causing_accident;

        -- 交換完以後、刪掉舊表
        DROP TABLE analysis_pesdestrian_causing_accident_deprecated;

    ELSE
        -- 如果不存在直接rename tmp表為正式表
        RENAME TABLE 
            analysis_pesdestrian_causing_accident_tmp TO analysis_pesdestrian_causing_accident;
    END IF;

END;

-- DELIMITER ;

-- DELIMITER %%
CREATE PROCEDURE swap_analysis_table2()
BEGIN
	DECLARE table_exists2 INT DEFAULT 0;

	CREATE TABLE IF NOT EXISTS analysis_pesdestrian_involving_accident_tmp
		AS (SELECT * FROM v4_v3_dimday_factenv_v1);

	SELECT count(*) INTO table_exists2
		FROM information_schema.tables
			WHERE table_schema = DATABASE()
				AND table_name = "analysis_pesdestrian_involving_accident";

	IF table_exists2 > 0 THEN
		RENAME TABLE 
			analysis_pesdestrian_involving_accident TO analysis_pesdestrian_involving_accident_deprecated,
			analysis_pesdestrian_involving_accident_tmp TO analysis_pesdestrian_involving_accident;
	
	ELSE
		RENAME TABLE
			analysis_pesdestrian_involving_accident_tmp TO analysis_pesdestrian_involving_accident;
	
	END IF;
END;

-- DELIMITER ;

CALL swap_analysis_table();
CALL swap_analysis_table2();
DROP TABLE IF EXISTS analysis_pesdestrian_causing_accident_deprecated;
DROP TABLE IF EXISTS analysis_pesdestrian_involving_accident_deprecated;

DROP VIEW IF EXISTS v1_dim_accident_type, v2_fact_accident_human, 
		   			v3_factmain_v2human, v4_v3_dimday_factenv_v1;

DROP PROCEDURE IF EXISTS swap_analysis_table;
DROP PROCEDURE IF EXISTS swap_analysis_table2;