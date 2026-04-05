-- 建立各季度行人涉入車禍案件中傷亡人數的分析表
-- 1. 篩選車禍當事者有行人的資料列
CREATE OR REPLACE VIEW v1_accident_human_vehicle AS
	(SELECT * FROM fact_accident_human
			WHERE vehicle_type_minor like "行人");

CREATE OR REPLACE VIEW v2_accident_human_vehicle_rn1 AS
	(SELECT tmp.* FROM 
		(SELECT *, ROW_NUMBER() OVER (PARTITION BY accident_id) AS rn
					FROM v1_accident_human_vehicle) tmp
			WHERE tmp.rn = 1);

-- 2. 由於需要車禍日期、時間、死傷人數，所以從main與day JOIN過來
CREATE OR REPLACE VIEW v3_accident_human_vehicle_rn1_main AS 
	(SELECT v2.vehicle_type_minor, m.*
		FROM fact_accident_main m -- 大表JOIN小表
			INNER JOIN v2_accident_human_vehicle_rn1 v2
				ON m.accident_id = v2.accident_id);
                
CREATE OR REPLACE VIEW v4_accident_human_vehicle_rn1_main_day AS
	(SELECT v3.vehicle_type_minor,
			d.accident_date, 
            v3.death_count,
            v3.injury_count
		FROM v3_accident_human_vehicle_rn1_main v3
			JOIN dim_accident_day d
				ON v3.day_id = d.day_id);

-- 3. 建立Mart層圖表
CREATE PROCEDURE swap_analysis_table()
BEGIN
	-- 宣告變數table_exists，初始化值為0
    DECLARE table_exists INT DEFAULT 0;


	CREATE TABLE IF NOT EXISTS mart_monthly_pedestrian_dj_tmp AS
	(SELECT
		YEAR(accident_date) AS `year`,
		QUARTER(accident_date) AS `quarter`,
		MONTH(accident_date) AS `month`,
		SUM(death_count) AS death_monthly_total,
		SUM(injury_count) AS injury_monthly_total
			FROM v4_accident_human_vehicle_rn1_main_day
				GROUP BY `year`, `quarter`, `month`
					ORDER BY `year`, `quarter`);


	ALTER TABLE mart_monthly_pedestrian_dj_tmp
		ADD COLUMN (
					avg_monthly_death_btw_2021_2025 DECIMAL(10,1), 
					stdev_monthly_death_btw_2021_2025 DECIMAL(10,1),
					avg_monthly_injury_btw_2021_2025 DECIMAL(10,1),
					stdev_monthly_injury_btw_2021_2025 DECIMAL(10,1),
					`avg+stdev_monthly_death_btw_2021_2025` DECIMAL(10,1),
					`avg-stdev_monthly_death_btw_2021_2025` DECIMAL(10,1),
					`avg+stdev_monthly_injury_btw_2021_2025` DECIMAL(10,1),
					`avg-stdev_monthly_injury_btw_2021_2025` DECIMAL(10,1)
					);

	SELECT
		@avg_death := AVG(death_monthly_total),
		@stdev_death := STDDEV(death_monthly_total),
		@avg_injury := AVG(injury_monthly_total),
		@stdev_injury := STDDEV(injury_monthly_total),
		@`avg+sd_death` := AVG(death_monthly_total) + STDDEV(death_monthly_total),
		@`avg-sd_death` := AVG(death_monthly_total) - STDDEV(death_monthly_total),
		@`avg+sd_injury` := AVG(injury_monthly_total) + STDDEV(injury_monthly_total),
		@`avg-sd_injury` := AVG(injury_monthly_total) - STDDEV(injury_monthly_total)
		FROM mart_monthly_pedestrian_dj_tmp
		WHERE `year` BETWEEN 2021 AND 2025;

	UPDATE mart_monthly_pedestrian_dj_tmp
		SET 
			avg_monthly_death_btw_2021_2025 = @avg_death,
			stdev_monthly_death_btw_2021_2025 = @stdev_death,
			avg_monthly_injury_btw_2021_2025 = @avg_injury,
			stdev_monthly_injury_btw_2021_2025 = @stdev_injury,
			`avg+stdev_monthly_death_btw_2021_2025` = @`avg+sd_death`,
			`avg-stdev_monthly_death_btw_2021_2025` = @`avg-sd_death`,
			`avg+stdev_monthly_injury_btw_2021_2025` = @`avg+sd_injury`,
			`avg-stdev_monthly_injury_btw_2021_2025` = @`avg-sd_injury`;

	-- 檢查正式表(非_tmp表)是否存在，並將查詢結果寫入table_exists，如果存在，count(*)會是1
    SELECT COUNT(*) INTO table_exists
    	FROM information_schema.tables
    		WHERE table_schema = DATABASE()
      			AND table_name = "mart_monthly_pedestrian_dj";


    -- IF/ELSE條件判斷
    IF table_exists > 0 THEN

        -- 如果存在做table swap
        RENAME TABLE 
            mart_monthly_pedestrian_dj TO mart_monthly_pedestrian_dj_deprecated,
            mart_monthly_pedestrian_dj_tmp TO mart_monthly_pedestrian_dj;

        -- 交換完以後、刪掉舊表
        DROP TABLE mart_monthly_pedestrian_dj_deprecated;

    ELSE
        -- 如果不存在直接rename tmp表為正式表
        RENAME TABLE 
            mart_monthly_pedestrian_dj_tmp TO mart_monthly_pedestrian_dj;
    END IF;
END;

CALL swap_analysis_table();
DROP TABLE IF EXISTS mart_monthly_pedestrian_dj_deprecated;

DROP VIEW IF EXISTS v1_accident_human_vehicle, v2_accident_human_vehicle_rn1,
		  v3_accident_human_vehicle_rn1_main, v4_accident_human_vehicle_rn1_main_day;

DROP PROCEDURE IF EXISTS swap_analysis_table;