create database factory_analytics;
use factory_analytics;

    ## creating table
CREATE TABLE plc_machine_data (
    time_stamp DATETIME,
    machine_id VARCHAR(20),
    machine_status VARCHAR(20),
    alarm_code VARCHAR(20),
    temperature_c DECIMAL(5,2),
    vibration_mm_s DECIMAL(5,2)
);

create table production_data(
date date,
machine_id varchar(20),
shift char(1),
planned_units int,
actual_units int,
ideal_cycle_time_min decimal(5,2)
);

CREATE TABLE quality_data (
    date DATE,
    machine_id VARCHAR(20),
    shift CHAR(1),
    good_units INT,
    rejected_units INT,
    defect_type VARCHAR(50)
);

CREATE TABLE maintenance_data (
    machine_id VARCHAR(20),
    failure_date DATE,
    failure_type VARCHAR(50),
    downtime_minutes INT,
    maintenance_action VARCHAR(50)
);

truncate plc_machine_data

SELECT COUNT(*) FROM plc_machine_data;
select count(*) from maintenance_data
SET GLOBAL local_infile = 1;
TRUNCATE TABLE plc_machine_data;

## loading data

LOAD DATA LOCAL INFILE 'C:/Users/jasir/OneDrive/Desktop/Analysis/Factory Data/maintenance_data.csv'
INTO TABLE maintenance_data
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
SHOW VARIABLES LIKE 'local_infile';
SET GLOBAL local_infile = 1;
SHOW VARIABLES LIKE 'local_infile';

select * from quality_data

### DOWNTIME CALCULATION
create view downtime_view as
SELECT 
    machine_id,
    COUNT(*) * 5 AS downtime_minutes
FROM plc_machine_data
WHERE machine_status = 'Fault'
GROUP BY machine_id;

### PRODUCTION SUMMARY
SELECT 
    machine_id,
    SUM(actual_units) AS total_production
FROM production_data
GROUP BY machine_id;

### QUALITY RATE & VIEW
CREATE or replace VIEW QUALITY_VIEW AS
SELECT 
    machine_id,
    SUM(good_units) / (SUM(good_units) + SUM(rejected_units))AS quality_rate
FROM quality_data
GROUP BY machine_id;

### AVAILABILITY & VIEW
CREATE VIEW availability_view AS
SELECT
    p.machine_id,
    (p.planned_time_minutes - d.downtime_minutes) / p.planned_time_minutes
        AS availability
FROM
    (
        SELECT machine_id, COUNT(*) * 480 AS planned_time_minutes
        FROM production_data
        GROUP BY machine_id
    ) p
JOIN
    (
        SELECT machine_id, COUNT(*) * 5 AS downtime_minutes
        FROM plc_machine_data
        WHERE machine_status = 'Fault'
        GROUP BY machine_id
    ) d
ON p.machine_id = d.machine_id;

### PERFORMANCE & VIEW
CREATE VIEW PERFORMANCE_VIEW AS
SELECT
    p.machine_id,
    (p.total_units * p.ideal_cycle_time_min) / r.run_time_minutes
        AS performance
FROM
    (
        SELECT
            machine_id,
            SUM(actual_units) AS total_units,
            AVG(ideal_cycle_time_min) AS ideal_cycle_time_min
        FROM production_data
        GROUP BY machine_id
    ) p
JOIN
    (
        SELECT
            machine_id,
            (COUNT(*) * 480 - COUNT(CASE WHEN machine_status = 'Fault' THEN 1 END) * 5)
                AS run_time_minutes
        FROM plc_machine_data
        GROUP BY machine_id
    ) r
ON p.machine_id = r.machine_id;

create or replace  view oee_view as
SELECT
    a.machine_id,
    a.availability * p.performance * q.quality_rate AS oee
FROM
    availability_view a
JOIN
    performance_view p ON a.machine_id = p.machine_id
JOIN
    quality_view q ON a.machine_id = q.machine_id;
    
    
    SHOW FULL TABLES WHERE Table_type = 'VIEW';
    use factory_analytics