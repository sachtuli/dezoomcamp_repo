## Week 1 Homework Solutions

## Question 1. Google Cloud SDK

* `Google Cloud SDK 369.0.0`
* `bq 2.0.72`  
* `core 2022.01.14`
* `gsutil 5.6`

## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output to the form


## Question 3. Count records 

How many taxi trips were there on January 15?

```sql
SELECT count(1) FROM yellow_taxi_data WHERE (select date(tpep_pickup_datetime) = '2021-01-15')
```
`Result : 53024`

## Question 4. Average

Find the largest tip for each day. 
On which day it was the largest tip in January?
(note: it's not a typo, it's "tip", not "trip")

```sql
`SELECT  date(tpep_pickup_datetime) as travel_date , max(tip_amount) as tip  FROM yellow_taxi_data WHERE date(tpep_pickup_datetime) >= '2021-01-01' AND date(tpep_pickup_datetime) < '2021-01-31' group by date(tpep_pickup_datetime)`
```
`
Largest Tip : 1140.44
Result : 2021-01-20	
`


## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Enter the district name (not id)

```sql
select y."DOLocationID" as drop_location_ID,count(y."DOLocationID") as drop_count from yellow_taxi_data y left outer join zones z on y."PULocationID" = z."LocationID" WHERE  date(tpep_pickup_datetime) = '2021-01-14' AND z."Zone" = 'Central Park' group by drop_location_ID order by drop_count desc`
```
`Result: Upper East Side South`


## Question 6. 

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

```sql
select concat(coalesce(zpu."Zone", 'Unknown'), ' / ', coalesce(zdo."Zone", 'Unknown')) as "route",
    avg(y."total_amount") as "avg_price" from
    yellow_taxi_data y
    join zones zpu on y."PULocationID" = zpu."LocationID"
    join zones zdo on y."DOLocationID" = zdo."LocationID"
group by "route" order by  "avg_price" desc limit 1
```

`Result: "Alphabet City / Unknown"`

