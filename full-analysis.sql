USE shootings;

# Data Cleaning & Merging
CREATE TABLE `wapo_shootings_2` (
	`id` int,
  `date` text,
  `threat_type` text,
  `flee_status` text,
  `armed_with` text,
  `city` text,
  `county` text,
  `victim_state` text,
  `latitude` double DEFAULT NULL,
  `longitude` double DEFAULT NULL,
  `location_precision` text,
  `victim_name` text,
  `age` int DEFAULT NULL,
  `gender` text,
  `race` text,
  `race_source` text,
  `was_mental_illness_related` text,
  `body_camera` text,
  `agency_ids` int DEFAULT NULL);

LOAD DATA LOCAL INFILE "C:\\Users\\nahia\\Downloads\\fatal-police-shootings-data.csv"
INTO TABLE shootings.wapo_shootings_2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

DROP TABLE IF EXISTS merged;
DROP TABLE IF EXISTS wapo_shootings;
RENAME TABLE wapo_shootings_2 to wapo_shootings;

SELECT * FROM shootings.wapo_shootings;

ALTER TABLE wapo_shootings
RENAME COLUMN name TO victim_name,
RENAME COLUMN state to victim_state
;

ALTER TABLE wapo_shootings
DROP COLUMN id;

CREATE TABLE merged AS
SELECT *
FROM shootings.wapo_shootings
LEFT JOIN shootings.agencies
ON wapo_shootings.agency_ids = agencies.id;

ALTER TABLE merged
DROP COLUMN id, /* dropping the duplicate agency id column */
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY,
MODIFY COLUMN date DATE;

SELECT * FROM merged;

# Data Analysis

## Incident Trends Over Time

SELECT YEAR(date) AS year, MONTH(date) AS month, COUNT(*) AS incident_count
FROM merged
GROUP BY YEAR(date), MONTH(date)
ORDER BY YEAR(date), MONTH(date);

SELECT YEAR(date) AS year, COUNT(*) AS incident_count
FROM merged
GROUP BY YEAR(date)
ORDER BY YEAR(date); #2023 had the highest number of incidents

## Demographic Analysis
### Age
SELECT AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age
FROM merged;

### Race
SELECT 
    race,
    COUNT(*) AS incident_count,
    ROUND((COUNT(*) / (SELECT COUNT(*) FROM merged) * 100), 2) AS percentage
FROM merged
WHERE race IS NOT NULL AND race <> ''
GROUP BY race
ORDER BY percentage DESC;

CREATE VIEW race_distribution AS
SELECT 
    CASE
        WHEN race IN ('W', 'B', 'H', 'A', 'N') THEN race
        ELSE 'other/multi'
    END AS categorized_race,
    COUNT(*) AS incident_count,
    ROUND((COUNT(*) / (SELECT COUNT(*) FROM merged) * 100), 2) AS percentage
FROM merged
WHERE race IS NOT NULL AND race <> ''
GROUP BY categorized_race
ORDER BY percentage DESC;

#### Merge in US Census data to assess disproportionality
CREATE VIEW race_disparity AS
SELECT
    r.categorized_race,
    r.incident_count,
    r.percentage AS shooting_per,
    p.pop,
    ROUND((r.incident_count / p.pop * 100), 10) AS shooting_per_of_total,
    ROUND(((r.incident_count / total_shootings.total) / (p.pop / total_population.total) * 100), 2) AS disparity_index
FROM race_distribution r
JOIN census p ON r.categorized_race = p.race
JOIN (
    SELECT COUNT(*) AS total
    FROM merged
    WHERE race IS NOT NULL AND race <> ''
) AS total_shootings ON TRUE
JOIN (
    SELECT SUM(pop) AS total
    FROM census
) AS total_population ON TRUE
ORDER BY disparity_index DESC; #Native American and Black populations experience disproportionate shootings

### Gender
SELECT 
    gender,
    COUNT(*) AS incident_count,
    ROUND((COUNT(*) / (SELECT COUNT(*) FROM merged) * 100), 2) AS percentage
FROM merged
WHERE gender IS NOT NULL
GROUP BY gender
ORDER BY percentage DESC;

## Threats
SELECT threat_type, COUNT(*) AS incident_count
FROM merged
GROUP BY threat_type
ORDER BY incident_count DESC;

### Flee status vs Threat Type (Cross Tabulate)
SELECT flee_status, threat_type, COUNT(*) AS incident_count
FROM merged
GROUP BY flee_status, threat_type
ORDER BY flee_status, incident_count DESC;

## Body Cameras
SELECT body_camera, COUNT(*) AS incident_count
FROM merged
GROUP BY body_camera;

## Mental Illness & Threats
SELECT was_mental_illness_related, threat_type, COUNT(*) as incident_count
FROM merged
GROUP BY  was_mental_illness_related, threat_type
ORDER BY was_mental_illness_related, incident_count DESC;

## Frequency by Locations

### Which states have the highest percentage of incidents?
SELECT 
    victim_state,
    COUNT(*) AS incident_count,
    ROUND((COUNT(*) / (SELECT COUNT(*) FROM merged) * 100), 2) AS percentage
FROM merged
WHERE victim_state IS NOT NULL
GROUP BY victim_state
ORDER BY percentage DESC; ## CA, TX, FL are top 3

## Export race_disparity and merged data for use in Tableau

