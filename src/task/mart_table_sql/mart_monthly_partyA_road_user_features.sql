-- 針對與行人直接相關的車禍案件，分析第一肇事者的(它可能是行人也可能是駕駛人)的用路行為
-- 1. 先串接出第一肇事者所需的欄位：個人身份特徵與當下用路行為。
CREATE OR REPLACE VIEW v1_humansq1 AS
	(SELECT *
		FROM fact_accident_human h
			WHERE is_primary_party_sequence = 1);

-- 2. 然後留下主要肇因關係有包含人的，因此透過main表取用type表
CREATE OR REPLACE VIEW v2_humansq1_main_type AS
	(SELECT m.accident_id,
			v1.gender,
            v1.age,
            v1.vehicle_type_major,
            t.accident_type_major,
            m.day_id
		FROM fact_accident_main m -- 大表JOIN小表
			JOIN v1_humansq1 v1
				ON m.accident_id = v1.accident_id
			JOIN dim_accident_type t
				ON m.accident_type_id = t.accident_type_id
					WHERE t.accident_type_major LIKE "%人" OR
						  t.accident_type_major LIKE "%人%" OR
						  t.accident_type_major LIKE "人%");

-- 3. 由於之後需要製作時間曲線，因此再串接day表
CREATE OR REPLACE VIEW v3_humansq1_main_type_day AS
	(SELECT v2.*, YEAR(d.accident_date) AS accident_year,
			QUARTER(d.accident_date) AS accident_quarter,
			MONTH(d.accident_date) AS accident_month
		FROM v2_humansq1_main_type v2
			JOIN dim_accident_day d
				ON d.day_id = v2.day_id);

-- 4. 將年齡分群轉為離散。將用路行為之一"車種"做分群。
CREATE OR REPLACE VIEW v4_humansq1_main_type_day_group AS
	SELECT
			accident_year, accident_quarter, accident_month,
            gender, age,
			CASE
				WHEN age < 18 AND age > 0 THEN "< 18歲"
				WHEN age < 20 AND age >= 18 THEN "18 & 19歲"
				WHEN age < 90 AND age >= 20 THEN CONCAT(ROUND(age/10)*10, " - ",
														(ROUND(age/10)*10+9), "歲")
				ELSE "未知"
			END AS age_grouped,

			CASE
				WHEN vehicle_type_major LIKE "小客車" THEN "小客車(含客、貨兩用)"
				WHEN vehicle_type_major LIKE "小貨車" THEN "小貨車(含客、貨兩用)"
				ELSE vehicle_type_major
			END AS vehicle_type_grouped,

			CASE
				WHEN age < 18 THEN "未成年"
				WHEN age >= 18 AND age < 65 THEN "青壯年者"
				WHEN age >= 65 AND age < 90 THEN "銀髮族"
				ELSE "未知"
			END AS age_cluster,

			CASE
				WHEN accident_type_major = '人與汽(機)車' THEN '人與車'
				WHEN accident_type_major = '人與汽機車' THEN '人與車'
				ELSE accident_type_major
			END AS accident_type_major_grouped
		FROM v3_humansq1_main_type_day;

-- 3. 逐年計算各年齡區間的平均年齡(男女分開計算)、各車種駕駛人平均年齡(各年齡層分開計算)，正式存成Mart層


CREATE PROCEDURE swap_analysis_table()
BEGIN
	-- 宣告變數table_exists，初始化值為0
    DECLARE table_exists INT DEFAULT 0;

    -- 建立 tmp 表
    CREATE TABLE IF NOT EXISTS mart_monthly_partyA_road_user_features_tmp AS
	WITH partitioned_avgs AS (
		SELECT
				accident_year,
				accident_quarter,
				accident_month,
				gender,
				age_grouped,
				age_cluster,
				vehicle_type_grouped,
				accident_type_major_grouped,
				AVG(age) OVER (PARTITION BY accident_year, accident_month,
											gender, age_grouped
							  ) as `各月度各年齡區間的平均年齡(男女分開計算)`,
				AVG(age) OVER (PARTITION BY accident_year, accident_month,
											age_grouped, vehicle_type_grouped
							  ) as `各月度各車種駕駛人平均年齡(各年齡層分開計算)`
			FROM v4_humansq1_main_type_day_group
				WHERE age_grouped != "未知" AND accident_type_major_grouped = "人與車"
		  )
	SELECT  accident_year,
			accident_quarter,
			accident_month,
            gender,
            age_grouped,
            age_cluster,
            vehicle_type_grouped,
            accident_type_major_grouped,
			`各月度各年齡區間的平均年齡(男女分開計算)`,
			`各月度各車種駕駛人平均年齡(各年齡層分開計算)`
            FROM partitioned_avgs
				GROUP BY accident_year, accident_quarter, accident_month,
						 gender, age_grouped, age_cluster, vehicle_type_grouped,
						 accident_type_major_grouped, `各月度各年齡區間的平均年齡(男女分開計算)`,
						 `各月度各車種駕駛人平均年齡(各年齡層分開計算)`;



    -- 檢查正式表(非_tmp表)是否存在，並將查詢結果寫入table_exists，如果存在，count(*)會是1
    SELECT COUNT(*) INTO table_exists
    	FROM information_schema.tables
    		WHERE table_schema = DATABASE()
      			AND table_name = "mart_monthly_partyA_road_user_features";


    -- IF/ELSE條件判斷
    IF table_exists > 0 THEN
		RENAME TABLE
			mmart_monthly_partyA_road_user_features to mart_monthly_partyA_road_user_features_deprecated,
			mart_monthly_partyA_road_user_features_tmp to mart_monthly_partyA_road_user_features;

	ELSE
		RENAME TABLE
			mart_monthly_partyA_road_user_features_tmp to mart_monthly_partyA_road_user_features;
	END IF;
END;

CALL swap_analysis_table();

DROP TABLE IF EXISTS mart_monthly_partyA_road_user_features_deprecated;

DROP VIEW IF EXISTS v1_humansq1, v2_humansq1_main_type,
					v3_humansq1_main_type_day, v4_humansq1_main_type_day_group;

DROP PROCEDURE IF EXISTS swap_analysis_table;
