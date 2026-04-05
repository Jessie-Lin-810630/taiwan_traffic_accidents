-- 建立各季度各類型車禍致死人數的分析表
-- 1. 將車禍大類別分得更乾淨
CREATE OR REPLACE VIEW v1_dim_accident_type AS
	(SELECT
		accident_type_id, 
        accident_type_major, 
		CASE
			WHEN accident_type_major = '人與汽(機)車' THEN '人與車'
			WHEN accident_type_major = '人與汽機車' THEN '人與車'
			WHEN accident_type_major = '汽(機)車本身' THEN '車輛本身'
			ELSE accident_type_major
		END AS accident_type_major_grouped
			FROM dim_accident_type);
            
-- 2. 由於需要車禍日期、死傷人數，所以取用main與day表
CREATE OR REPLACE VIEW  v2_typegroup_main_day AS
	(SELECT v1.accident_type_major_grouped,
			YEAR(d.accident_date) AS accident_year,
			QUARTER(d.accident_date) AS accident_quarter,
			m.accident_time,
			m.death_count, 
            m.injury_count
		FROM fact_accident_main m -- 大表JOIN小表
			JOIN v1_dim_accident_type v1
			ON m.accident_type_id = v1.accident_type_id
					JOIN dim_accident_day d
					ON m.day_id = d.day_id);

-- 3. 建立Mart層圖表
CREATE PROCEDURE swap_analysis_table()
BEGIN
	-- 宣告變數table_exists，初始化值為0
    DECLARE table_exists INT DEFAULT 0;

	CREATE TABLE IF NOT EXISTS mart_quarterly_death_tmp AS
		(SELECT
			accident_year, accident_quarter,
			accident_type_major_grouped,
			SUM(death_count) AS `death_quarterly_counts`
				FROM v2_typegroup_main_day v2
					GROUP BY accident_year, accident_quarter, accident_type_major_grouped
						ORDER BY accident_year, accident_quarter);

	-- 檢查正式表(非_tmp表)是否存在，並將查詢結果寫入table_exists，如果存在，count(*)會是1
    SELECT COUNT(*) INTO table_exists
    	FROM information_schema.tables
    		WHERE table_schema = DATABASE()
      			AND table_name = "mart_quarterly_death";


    -- IF/ELSE條件判斷
    IF table_exists > 0 THEN

        -- 如果存在做table swap
        RENAME TABLE 
            mart_quarterly_death TO mart_quarterly_death_deprecated,
            mart_quarterly_death_tmp TO mart_quarterly_death;

        -- 交換完以後、刪掉舊表
        DROP TABLE mart_quarterly_death_deprecated;

    ELSE
        -- 如果不存在直接rename tmp表為正式表
        RENAME TABLE 
            mart_quarterly_death_tmp TO mart_quarterly_death;
    END IF;
END;

CALL swap_analysis_table();
DROP TABLE IF EXISTS mart_quarterly_death_deprecated;

DROP VIEW IF EXISTS v1_dim_accident_type, v2_typegroup_main_day;

DROP PROCEDURE IF EXISTS swap_analysis_table;