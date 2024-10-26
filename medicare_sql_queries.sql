-- PRELIMINARY DATA EXPLORATION

-- Table: medical_events
-- What is the date range in the data?
SELECT MIN(end_date) AS date_range
FROM `core.medical_events_clean`
UNION ALL
SELECT MAX(end_date) AS date_range
FROM `core.medical_events_clean`
ORDER BY date_range;
-- The data spans about a 2-year period, with records spanning from 2020-02-03 to 2022-01-26.

-- What are the different kinds of event types, and how many of each are there?
SELECT event_subtype_name, 
    COUNT(event_subtype_name) AS event_count,
    SUM(COUNT(event_subtype_name)) OVER () AS total_count,
    ROUND(COUNT(event_subtype_name) / (SUM(COUNT(event_subtype_name)) OVER ()) * 100, 1)  AS pct
  FROM `core.medical_events_clean`
  GROUP BY 1
  ORDER BY 4 DESC;
-- PCP visits are by far the most frequent event (~116,000) and represent 77.2% of all visits.
-- ED visits are the second most frequent event (29,000+) and represent 19.3% of all visits.
-- Admissions are the least frequent event (5,200+) and represent just 3.5% of all visits..

-- What is the median duration of each type of visit?
WITH duration_data AS (
  SELECT event_subtype_name,
    DATE_DIFF(end_date, start_date, DAY) AS duration_days
  FROM `core.medical_events_clean`
)

SELECT DISTINCT event_subtype_name,
  PERCENTILE_CONT(duration_days, 0.5) OVER (PARTITION BY event_subtype_name) AS median_days,
FROM duration_data;
-- The median duration in days of Admission events is 4 days, but 0 days for both ED Visits and PCP Visits.


-- Table: chronic_and_bh
-- What are the different kinds of diseases that are in the data and what is their frequency?
WITH disease_counts AS (
  SELECT disease_group,
    COUNT (disease_group) AS disease_count,
    SUM(COUNT (disease_group)) OVER () AS total_count
  FROM `core.chronic_and_bh_clean`
  GROUP BY 1)

SELECT *,
  ROUND(disease_count / total_count * 100, 1) AS pct
FROM disease_counts
ORDER BY 4 DESC;
-- Anxiety disorders, depression, and stress reactions are the 3 most commonly diagnosed diseases, together making up ~17% of diagnoses.

-- Are there members who get diagnosed with the same disease but in separate time periods?
WITH recurrent_diagnoses AS (
  SELECT member_profile_id, 
    disease_group,
    COUNT(disease_group) AS num_diagnosis
  FROM `core.chronic_and_bh_clean`
  GROUP BY 1, 2
  ORDER BY num_diagnosis DESC)

SELECT disease_group,
  AVG(num_diagnosis) AS avg_num_diagnosis
FROM recurrent_diagnoses
GROUP BY 1
ORDER BY 2 DESC;
-- Heart disease is the most recurrently diagnosed disease in members.


-- IDENTIFYING TIER 1 and TIER 2 MEMBERS
/* 
Step 1: Identify members who are not eligible for the ED Diversion program because of an Admission event in the last 12 months (on or after 2021-01-26) using a CTE.
Step 2: Use a second CTE to remove the members identified in above step by using a subquery within the WHERE clause, leaving eligible members.
Step 3: In a third CTE, set a window of 365 days partitioned on member_profile_id and event_subtype to get the rolling count of each type of visit within a 365 day window.
Step 4: In a fourth CTE, define the ED utilization rate by filtering for rows corresponding to ED visits and getting the maximum rolling count calculated in the third step, for each member. 
Step 5: Use a fifth CTE to join the ED utilization rates calculated in Step 4 back to the eligible members isolated in Step 2, on member_profile_id. Because some of these members didn't have any ED visits, and therefore no ED utilization rate, use a CASE WHEN clause to replace NULL values with 0. Use DISTINCT to return one row for each member.
Step 6: In the final CTE, assign a tier to each member based on their ED utilization rate, ed_rate. Based on statistical analysis on the distribution of ED utilization rates carried out in a separate notebook, the thresholds are set at >= 3 visits for Tier 2 and >= 8 visits for Tier 1, with remaining members categorized as Low.
Step 6: Members are now stratified into Tier 1 and Tier 2, and can be isolated using the WHERE clause. At this point, the data can be exported to a separate .csv file for downstream analysis.
*/ 

WITH excluded_members AS (
    SELECT member_profile_id
    FROM `core.medical_events_clean`
    WHERE event_subtype_name = 'Admission' AND start_date >= '2021-01-26'
  ),

  eligible_members AS (
    SELECT member_profile_id,
      event_subtype_name,
      start_date
    FROM `core.medical_events_clean`
    WHERE member_profile_id NOT IN (SELECT member_profile_id FROM excluded_members)
  ),

  rolling_counts AS (
    SELECT member_profile_id, 
      event_subtype_name,
      COUNT(*) OVER (PARTITION BY member_profile_id, event_subtype_name 
                    ORDER BY UNIX_DATE(start_date)
                    RANGE BETWEEN 364 PRECEDING AND CURRENT ROW) AS rolling_count
    FROM eligible_members
  ),

  ed_rates AS (
    SELECT member_profile_id,
      MAX(rolling_count) AS ed_rate
    FROM rolling_counts
    WHERE event_subtype_name = 'ED Visit'
    GROUP BY 1
  ),

  joined_tbl AS (
    SELECT DISTINCT eligible_members.member_profile_id,
      CASE WHEN ed_rates.ed_rate IS NULL THEN 0 ELSE ed_rates.ed_rate END AS ed_rate
    FROM eligible_members
    LEFT JOIN ed_rates
      ON eligible_members.member_profile_id = ed_rates.member_profile_id
  ),

  tiers AS (
    SELECT *,
      CASE WHEN ed_rate >= 8 THEN 'Tier 1'
        WHEN ed_rate >= 3 THEN 'Tier 2'
        ELSE 'Low' END AS tier
    FROM joined_tbl
  )

SELECT * 
FROM tiers
WHERE tier IN ('Tier 1', 'Tier 2');
