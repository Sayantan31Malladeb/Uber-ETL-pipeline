SAYANTAN MALLADEB, sm103@illinois.edu

##1. What is the average trip distance by vendor for the given dataset?##

WITH VendorAvgDistance AS (
  SELECT
    VendorID,
    ROUND(AVG(trip_distance),3) AS avg_distance
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    VendorID
)
SELECT * FROM VendorAvgDistance;



##2. What is the percentage of cash vs. credit card payments for each vendor?##

WITH PaymentTypePercentage AS (
  SELECT
    VendorID,
    payment_type_name,
    COUNT(*) AS payment_count,
    ROUND((COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY VendorID))*100,3) AS payment_percentage
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    VendorID, payment_type_name
)
SELECT * FROM PaymentTypePercentage;



##3. What are the peak hours for trips during the day?##

WITH HourlyTripCount AS (
  SELECT
    EXTRACT(HOUR FROM TIMESTAMP(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S',tpep_pickup_datetime))) AS pickup_hour,
    COUNT(*) AS trip_count
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    pickup_hour
)
SELECT * FROM HourlyTripCount
ORDER BY
  trip_count DESC
LIMIT 30;

  


##4. How does trip fare vary based on payment type?##

SELECT
  payment_type_name,
  ROUND(AVG(fare_amount),3) AS avg_fare
FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
GROUP BY
  payment_type_name
ORDER BY
  avg_fare DESC;



##5. What are the top 10 trips with the highest tip amounts?##

SELECT
  VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, rate_code_name, trip_distance, tip_amount,
  RANK() OVER (ORDER BY tip_amount DESC) AS tip_rank
FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
ORDER BY
  tip_rank
LIMIT 10;



##6. How does the total fare amount change over time?##

WITH HourlyAverageFare AS (
  SELECT
    EXTRACT(HOUR FROM TIMESTAMP(tpep_pickup_datetime)) AS pickup_hour,
    ROUND(AVG(fare_amount),3) AS avg_fare
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    pickup_hour
)
SELECT * FROM HourlyAverageFare
ORDER BY
  pickup_hour;



##7. What is the average trip distance for different rate codes?##

SELECT
  rate_code_name,
  ROUND(AVG(trip_distance),3) AS avg_distance
FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
GROUP BY
  rate_code_name;



##8. How does the tip amount vary based on the number of passengers in the trip?##

SELECT
  passenger_count,
  ROUND(AVG(tip_amount),2) AS avg_tip
FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
GROUP BY
  passenger_count
ORDER BY
  passenger_count;



##9. What is the overall trend in total tolls amount collected over time?##

WITH HourlyTotalTolls AS (
  SELECT
    EXTRACT(HOUR FROM TIMESTAMP(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_pickup_datetime))) AS pickup_hour,
    ROUND(SUM(tolls_amount),2) AS total_tolls
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    pickup_hour
)
SELECT * FROM HourlyTotalTolls
ORDER BY
  pickup_hour;



##10. Profitability by Hour?##

WITH HourlyProfit AS (
  SELECT
    EXTRACT(HOUR FROM TIMESTAMP(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_pickup_datetime))) AS pickup_hour,
    ROUND(SUM(total_amount),3) AS total_profit
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
  GROUP BY
    pickup_hour
)
SELECT * FROM HourlyProfit
ORDER BY
  pickup_hour;



##11. Ride Duration Analysis:##

SELECT
  trip_duration,
  COUNT(*) AS trip_count
FROM (
  SELECT
    TIMESTAMP_DIFF(
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_dropoff_datetime),
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_pickup_datetime),
      SECOND
    ) AS trip_duration
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
)
GROUP BY
  trip_duration;



##12. Geographic Analysis: ##

SELECT
  ROUND(pickup_longitude, 2) AS pickup_longitude_rounded,
  ROUND(pickup_latitude, 2) AS pickup_latitude_rounded,
  ROUND(AVG(fare_amount),2) AS avg_fare_amount,
  ROUND(AVG(trip_duration),2) AS avg_trip_duration
FROM (
  SELECT
    pickup_longitude,
    pickup_latitude,
    TIMESTAMP_DIFF(
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_dropoff_datetime),
      PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', tpep_pickup_datetime),
      SECOND
    ) AS trip_duration,
    fare_amount
  FROM
    `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
)
GROUP BY
  pickup_longitude_rounded,
  pickup_latitude_rounded;



##13. Fare vs. Distance: ##

SELECT
  rate_code_name,
  ROUND(AVG(trip_distance),2) AS avg_trip_distance,
  ROUND(AVG(fare_amount),2) AS avg_fare_amount
FROM
  `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
GROUP BY
  rate_code_name;



##14. Rate Code Analysis: ##

SELECT
  rate_code_name,
  COUNT(*) AS rate_code_count,
  ROUND(AVG(fare_amount),2) AS avg_fare_amount,
  ROUND(SUM(total_amount),2) AS total_revenue
FROM
  `uber-pipeline-project.uber_data_pipeline.uber_analysis_table`
GROUP BY
  rate_code_name
