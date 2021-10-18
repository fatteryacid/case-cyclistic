-----------------------------
---- Case Study 1 in SQL ----
-- Presented by Tyler Tran --
-----------------------------

-- Goal: Use SQL to quickly clean & merge large datasets, and prepare for analysis.
-- Business task: How do annual members and casual riders use Cyclistic bikes differently?


--------------------------------------------------------------
-- Dataset I: Q1 2020 data from Cyclistic.
CREATE TABLE q1_2020 ( -- Create table for Q1 2020 data.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    start_lat NUMERIC,
    start_lng NUMERIC,
    end_lat NUMERIC,
    end_lng NUMERIC,
    member_casual VARCHAR(20)
);

COPY q1_2020 -- Importing Q1_2020 data from CSV file.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by quarter/raw-data/Divvy_Trips_2020_Q1.csv'
    WITH DELIMITER ','
    CSV HEADER
;

ALTER TABLE q1_2020 -- Remove geo data since not present in historical data for analysis.
    DROP COLUMN start_lat,
    DROP COLUMN start_lng,
    DROP COLUMN end_lat,
    DROP COLUMN end_lng
;


--------------------------------------------------------------
-- Dataset II: Q4 2019 data from Cyclistic.
CREATE TABLE q4_2019 ( -- Create table for Q4 2019 data.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    member_casual VARCHAR(20)
);

-- NOTE: This data has issues if attempting to import directly.
-- ISSUE: Comma(,) prevents importing columns as NUMERIC.
-- Please see solution below.

CREATE TABLE temp_table_q4 ( -- Creating a temporary table.
        trip_id TEXT NOT NULL PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        bikeid INT,
        tripduration VARCHAR(50), -- Storing problem values as VARCHAR
        from_station_id INT,
        from_station_name TEXT,
        to_station_id INT,
        to_station_name TEXT,
        usertype TEXT,
        gender TEXT,
        birthyear INT
);

COPY temp_table_q4 -- Importing CSV files into temp_table_q4
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by quarter/raw-data/Divvy_Trips_2019_Q4.csv'
    WITH DELIMITER ','
    CSV HEADER
;

ALTER TABLE temp_table_q4 -- Trip duration, gender and birth year no longer collected in 2020.
    DROP COLUMN gender,
    DROP COLUMN birthyear
    DROP COLUMN tripduration
;

INSERT INTO q4_2019 ( -- Populating q4_2019 table
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    member_casual
)
SELECT 
    trip_id, -- ride_id
    bikeid :: VARCHAR(100), -- rideable_type
    start_time, -- started_at
    end_time, -- ended_at
    from_station_name :: VARCHAR(200), -- start_station_name
    from_station_id :: VARCHAR(200), -- start_station_id
    to_station_name :: VARCHAR(200), -- end_station_name
    to_station_id :: VARCHAR(200), -- end_station_id
    usertype :: VARCHAR(20) -- member_casual
FROM temp_table_q4;

DROP TABLE temp_table_q4; -- Remove temporary table.


--------------------------------------------------------------
-- Dataset III: Q3 2019 data from Cyclistic.
CREATE TABLE q3_2019 ( -- Create table for Q3 2019 data.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    member_casual VARCHAR(20)
);

-- NOTE: This data has issues if attempting to import directly.
-- ISSUE: Comma(,) prevents importing columns as NUMERIC.
-- Please see solution below.

CREATE TABLE temp_table_q3 ( -- Creating a temporary table.
        trip_id TEXT NOT NULL PRIMARY KEY,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        bikeid INT,
        tripduration VARCHAR(50), -- Storing problem values as VARCHAR
        from_station_id INT,
        from_station_name TEXT,
        to_station_id INT,
        to_station_name TEXT,
        usertype TEXT,
        gender TEXT,
        birthyear INT
);

COPY temp_table_q3 -- Importing CSV files into temp_table_q4
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by quarter/raw-data/Divvy_Trips_2019_Q3.csv'
    WITH DELIMITER ','
    CSV HEADER
;

ALTER TABLE temp_table_q3 -- Trip duration, gender and birth year no longer collected in 2020.
    DROP COLUMN gender,
    DROP COLUMN birthyear,
    DROP COLUMN tripduration
;

INSERT INTO q3_2019 ( -- Populating q3_2019 table.
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    member_casual
)
SELECT 
    trip_id, -- ride_id
    bikeid :: VARCHAR(100), -- rideable_type
    start_time, -- started_at
    end_time, -- ended_at
    from_station_name :: VARCHAR(200), -- start_station_name
    from_station_id :: VARCHAR(200), -- start_station_id
    to_station_name :: VARCHAR(200), -- end_station_name
    to_station_id :: VARCHAR(200), -- end_station_id
    usertype :: VARCHAR(20) -- member_casual
FROM temp_table_q3;

DROP TABLE temp_table_q3; -- Remove temporary table.


--------------------------------------------------------------
-- Dataset IV: Q2 2019 data from Cyclistic.
CREATE TABLE q2_2019 ( -- Create table for Q2 2019 data.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    member_casual VARCHAR(20)
);

-- Fixing data type issue for q2_2019.
CREATE TABLE temp_table_q2 ( -- Creating a temporary table.
    "01 - Rental Details Rental ID" NUMERIC NOT NULL PRIMARY KEY,
    "01 - Rental Details Local Start Time" TIMESTAMP,
    "01 - Rental Details Local End Time" TIMESTAMP,
    "01 - Rental Details Bike ID" NUMERIC,
    "01 - Rental Details Duration In Seconds Uncapped" VARCHAR(50), -- Storing problem values as VARCHAR.
    "03 - Rental Start Station ID" NUMERIC,
    "03 - Rental Start Station Name" TEXT,
    "02 - Rental End Station ID" NUMERIC,
    "02 - Rental End Station Name" TEXT,
    "User Type" TEXT,
    "Member Gender" TEXT,
    "05 - Member Details Member Birthday Year" INT
);

COPY temp_table_q2 -- Importing CSV files into temp_table_q3
FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by quarter/raw-data/Divvy_Trips_2019_Q2.csv'
WITH DELIMITER ','
CSV HEADER;

ALTER TABLE temp_table_q2
    DROP COLUMN "01 - Rental Details Duration In Seconds Uncapped",
    DROP COLUMN "Member Gender",
    DROP COLUMN "05 - Member Details Member Birthday Year"
;

INSERT INTO q2_2019 -- Populating q2_2019 table.
SELECT 
    "01 - Rental Details Rental ID", -- ride_id
    "01 - Rental Details Bike ID" :: VARCHAR(100), -- rideable_type
    "01 - Rental Details Local Start Time", -- started_at
    "01 - Rental Details Local End Time", -- ended_at
    "03 - Rental Start Station Name" :: VARCHAR(200), -- start_station_name
    "03 - Rental Start Station ID" :: VARCHAR(200), -- start_station_id
    "02 - Rental End Station Name" :: VARCHAR(200), -- end_station_name
    "02 - Rental End Station ID" :: VARCHAR(200), --end_station_id
    "User Type" -- member_casual
FROM temp_table_q2;

DROP TABLE temp_table_q2; -- Remove temporary table.


--------------------------------------------------------------
-- Dataset V: Monthly data from 04-2020 to 06 2021.
CREATE TABLE cyclistic_04_2020_to_06_2021 ( -- Create table for remaining data.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    start_lat NUMERIC,
    start_lng NUMERIC,
    end_lat NUMERIC,
    end_lng NUMERIC,
    member_casual VARCHAR(20)
);

COPY cyclistic_04_2020_to_06_2021 -- 04-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202004-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 05-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202005-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 06-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202006-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 07-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202007-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 08-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202008-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 09-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202009-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 10-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202010-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 11-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202011-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 12-2020.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202012-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 01-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202101-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 02-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202102-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 03-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202103-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 04-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202104-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 05-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202105-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

COPY cyclistic_04_2020_to_06_2021 -- 06-2021.
    FROM '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/by month (legacy)/202106-divvy-tripdata.csv'
    WITH DELIMITER ','
    CSV HEADER
;

ALTER TABLE cyclistic_04_2020_to_06_2021 -- Remove geo data since not present in historical data for analysis.
    DROP COLUMN start_lat,
    DROP COLUMN start_lng,
    DROP COLUMN end_lat,
    DROP COLUMN end_lng
;


--------------------------------------------------------------
-- Combining all tables into 1 master table (Q1-2020 to Q2-2019).

CREATE TABLE all_data ( -- Creating aggregate table for all quarterly data sets.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    member_casual VARCHAR(20)
);

INSERT INTO all_data -- Using UNION to insert multiple tables together.
SELECT * FROM q1_2020
UNION
SELECT * FROM q4_2019
UNION
SELECT * FROM q3_2019
UNION
SELECT * FROM q2_2019
UNION
SELECT * FROM cyclistic_04_2020_to_06_2021
ORDER BY started_at DESC
;


--------------------------------------------------------------
-- Adding new columns for help in analysis
-- OBJECTIVE: Create new columns for: ride duration, day of week, year, month.

CREATE TABLE temp_table_dates (
    id TEXT REFERENCES all_data(ride_id),
    ride_duration INTERVAL,
    day_of_week VARCHAR(10),
    month VARCHAR(15),
    year INT
);

INSERT INTO temp_table_dates ( -- This table will store redundant values that help with visualization and analysis.
    id,
    ride_duration,
    day_of_week,
    month,
    year
)
SELECT
    ride_id, -- foreign key
    ended_at - started_at, -- ride_duration hh:mm:ss
    to_char(started_at, 'Day') :: VARCHAR(10), -- day_of_week
    to_char(started_at, 'Month') :: VARCHAR(15), -- month
    extract(year from started_at) :: INT-- year
FROM all_data
;

CREATE TABLE export_table ( -- To protect work, creating new table for data visualization.
    ride_id TEXT NOT NULL PRIMARY KEY,
    rideable_type VARCHAR(100),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    start_station_name VARCHAR(200),
    start_station_id VARCHAR(200),
    end_station_name VARCHAR(200),
    end_station_id VARCHAR(200),
    member_casual VARCHAR(20),
    temp_id TEXT,
    ride_duration INTERVAL,
    day_of_week VARCHAR(10),
    month VARCHAR(15),
    year INT
);

INSERT INTO export_table -- Bringing it all together now!
SELECT
    *
FROM all_data JOIN temp_table_dates ON (
    all_data.ride_id = temp_table_dates.id)
; 


--------------------------------------------------------------
-- Cleaning up export table
-- ISSUE 1: member_casual column has inconsistent variables (Subscriber, Customer, member, casual). We only want 'member' and 'casual'.
-- ISSUE 2: Certain rides logged were for QR purposes.
-- ISSUE 3: Some ride_durations have negative values.

-- Issue 1 Solution
UPDATE 
    export_table
SET 
    member_casual = REPLACE(member_casual,'Subscriber', 'member')
;


UPDATE
    export_table
SET
    member_casual = REPLACE(member_casual,'Customer', 'casual')
;

-- Issue 2 & 3 Solution
DELETE FROM export_table
WHERE (
    start_station_name = 'HQ QR' OR -- Internal QR tests.
    start_station_name LIKE '%TEST%' OR -- External bike tests.
    export_table.ride_duration < '00:00:00' :: INTERVAL -- Negative rides.
    
);


-- Cleaning up temporary field(s) and table(s).
ALTER TABLE export_table -- Remove unneeded temp_id column.
DROP COLUMN temp_id;

DROP TABLE temp_table_dates; -- Remove temporary table.




--------------------------------------------------------------
-- Exporting table for visualization

COPY (
    SELECT
        *
    FROM
        export_table 
)
TO
    '/Users/tylertran/Documents/Data Analyst Projects/Case Study 1 - Cyclistic/Data/exported/cleaned-cyclistic-data-v03.csv'
DELIMITER ','
CSV HEADER
;



