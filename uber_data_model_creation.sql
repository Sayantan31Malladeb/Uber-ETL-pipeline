CREATE OR REPLACE TABLE `uber-pipeline-project.uber_data_pipeline.uber_analysis_table` AS 
(
SELECT 
f.VendorID,
d.tpep_pickup_datetime,
d.tpep_dropoff_datetime, 
p.passenger_count,
r.rate_code_name, 
t.trip_distance,
pick.pickup_longitude,
pick.pickup_latitude, 
dr.dropoff_longitude,
dr.dropoff_latitude,
pay.payment_type_name,
pay.payment_type_id, 
f.fare_amount,
f.tip_amount,
f.extra, 
f.mta_tax,
f.tolls_amount,
f.improvement_surcharge,
f.total_amount
from `uber-pipeline-project.uber_data_pipeline.fact_table` f JOIN
`uber-pipeline-project.uber_data_pipeline.datetime_dim` d ON f.datetime_id = d.datetime_id

JOIN `uber-pipeline-project.uber_data_pipeline.pickup_location_dim` pick ON pick.pickup_location_id = f.pickup_location_id

JOIN `uber-pipeline-project.uber_data_pipeline.passenger_count_dim` p ON p.passenger_count_id=f.passenger_count_id

JOIN `uber-pipeline-project.uber_data_pipeline.payment_type_dim` pay ON pay.payment_type_id = f.payment_type_id

JOIN `uber-pipeline-project.uber_data_pipeline.rate_code_dim` r ON r.rate_code_id = f.rate_code_id

JOIN `uber-pipeline-project.uber_data_pipeline.trip_distance_dim` t ON t.trip_distance_id = f.trip_distance_id

JOIN `uber-pipeline-project.uber_data_pipeline.dropoff_location_dim` dr ON dr.dropoff_location_id = f.dropoff_location_id

)
;