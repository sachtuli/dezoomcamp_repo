CREATE OR REPLACE TABLE `clever-grid-339211.trips_data_all.fhv_trips_data_partition_clustered`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM `clever-grid-339211.trips_data_all.fhv_external_table`;

SELECT count(*) FROM `clever-grid-339211.trips_data_all.fhv_trips_data_partition_clustered`
WHERE DATE(dropoff_datetime) >= "2019-01-01" 
and DATE(dropoff_datetime) < "2019-03-31"
and dispatching_base_num in ('B00987', 'B02060', 'B02279');