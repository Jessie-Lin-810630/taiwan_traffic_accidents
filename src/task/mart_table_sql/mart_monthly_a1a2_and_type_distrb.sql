-- 創立各月度A1A2比例分佈分析資料表(步驟3)、人車肇事案件類別的案量比例分析(步驟4)
-- 1.先串出每個案件對應的月份，因此取用main & day表，建成view表
CREATE OR REPLACE VIEW v1_accident_main_day AS
	(SELECT m.*, YEAR(d.accident_date) AS accident_year,
			QUARTER(d.accident_date) AS accident_quarter,
			MONTH(d.accident_date) AS accident_month
		FROM fact_accident_main m
			JOIN dim_accident_day d
				ON m.day_id = d.day_id
	);

-- 2.再串連案件的category(A1/A2)與accident_type_major，因此取用type表，建成view表
CREATE OR REPLACE VIEW v2_accident_main_day_type AS
	(SELECT v1.*, d.accident_category,
			d.accident_type_major
		FROM v1_accident_main_day v1
			JOIN dim_accident_type d
				ON v1.accident_type_id = d.accident_type_id);

-- 3-1. 先建立月度基礎統計 A1/A2比例(不管是否涉及行人)，建成view表
CREATE OR REPLACE VIEW v3_accident_monthly_counts AS
	(SELECT accident_year, accident_quarter,
	   accident_month, accident_category,
       count(*) AS accident_counts,
       100 * count(*) / sum(count(*)) OVER ( PARTITION BY
										accident_year, accident_quarter, accident_month
									 ) AS `counts_ratio_in_month`
	FROM traffic_accidents.v2_accident_main_day_type
		GROUP BY accident_year, accident_quarter, accident_month, accident_category);

-- 3-2. 可再建立單一年度下，逐月累計事故案件數量，轉建成Mart表
DROP TABLE IF EXISTS mart_montly_a1a2_distrb;
CREATE TABLE IF NOT EXISTS mart_montly_a1a2_distrb AS
	(SELECT
			accident_year,
			accident_quarter,
			accident_month,
			accident_counts,
			accident_category,
			SUM(accident_counts) OVER (
				PARTITION BY accident_year, accident_category
				ORDER BY accident_month
			) AS running_counts_monthly
		FROM v3_accident_monthly_counts);


-- 4-1. 將車禍大類別分得更乾淨，作為view表。
CREATE OR REPLACE VIEW v4_accident_main_day_type_grouped AS
	(SELECT
		accident_year, accident_quarter, accident_month, accident_category,
		CASE
			WHEN accident_type_major = "人與汽(機)車" THEN "人與車"
			WHEN accident_type_major = "人與汽機車" THEN "人與車"
			WHEN accident_type_major = "汽(機)車本身" THEN "車輛本身"
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM v2_accident_main_day_type);

-- 4-2. 先建立月度基礎統計 各種accident_type_major，建成view表
CREATE OR REPLACE VIEW v5_accident_main_day_type_grouped_counts AS
	(SELECT accident_year, accident_quarter,
	   accident_month, accident_category, accident_type_major_grouped,
       count(*) AS accident_counts,
       100 * count(*) / sum(count(*)) OVER ( PARTITION BY accident_year,
														  accident_quarter,
														  accident_month
											 ORDER BY accident_year,
													  accident_month
										   ) AS `counts_ratio_in_month`
	FROM v4_accident_main_day_type_grouped
		GROUP BY accident_year, accident_quarter, accident_month, accident_category,
        accident_type_major_grouped);

-- 4.3 可再建立單一年度下，逐月累計事故案件數量，轉建成Mart表
CREATE PROCEDURE swap_analysis_table()
BEGIN
	DECLARE table_exists INT DEFAULT 0;

	CREATE TABLE IF NOT EXISTS mart_montly_accident_type_distrb_tmp AS
		(SELECT
				accident_year,
				accident_quarter,
				accident_month,
				accident_counts,
				accident_category,
				accident_type_major_grouped,
				SUM(accident_counts) OVER (
					PARTITION BY accident_year,
								accident_category,
								accident_type_major_grouped
					ORDER BY accident_month
				) AS running_counts_monthly
			FROM v5_accident_main_day_type_grouped_counts);

	SELECT count(*) INTO table_exists
		FROM information_schema.tables
			WHERE table_schema = DATABASE()
				AND table_name = "mart_montly_accident_type_distrb";

	IF table_exists > 0 THEN
		RENAME TABLE
			mart_montly_accident_type_distrb to mart_montly_accident_type_distrb_deprecated,
			mart_montly_accident_type_distrb_tmp to mart_montly_accident_type_distrb;

	ELSE
		RENAME TABLE
			mart_montly_accident_type_distrb_tmp to mart_montly_accident_type_distrb;
	END IF;
END;


CALL swap_analysis_table();
DROP TABLE IF EXISTS mart_montly_accident_type_distrb_deprecated;

DROP VIEW IF EXISTS v1_accident_main_day,
					v2_accident_main_day_type,
					v3_accident_monthly_counts,
					v4_accident_main_day_type_grouped,
					v5_accident_main_day_type_grouped_counts;

DROP PROCEDURE IF EXISTS swap_analysis_table;
