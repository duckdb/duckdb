# NYC Taxi Benchmark

This benchmark is based on the following blog posts:
- [Billion NYC Taxi Rides Redshift](https://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html)
- [Benchmarks](https://tech.marksblogg.com/benchmarks.html)

Following steps similar to those described in the original blog posts, we generated approximately 1.8 billion rides and split them into GZipped CSV files, each containing 20 million rows. This resulted in 91 files, with sizes ranging from 300 MB to 1.2 GB.

The files are stored and publicly available on our [blobs website](https://blobs.duckdb.org/data/nyc-taxi-dataset), and you can check the `benchmark/taxi/load.sql` for a detailed reference of the URLs.

For the benchmark queries, they are almost the same as the ones described in the [benchmark blog post](https://tech.marksblogg.com/benchmarks.html). The main difference is that we always order the results to guarantee a consistent output. This allows us to perform result-checking and ensure that the benchmark is accurate. Note that the results were initially generated using a PostgreSQL instance, which was used during the data generation step.

In the following sections, I will describe how we generated the 91 compressed CSV files.

## Data Generation
The data is generated in a similar way as described in the [billion-nyc-taxi-rides](https://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html) blog post.
We use the [nyc-taxi-data](https://github.com/toddwschneider/nyc-taxi-data) repository scripts to download the parquet files, transform them into CSV files, load them into PostgreSQL, and export them to one big CSV file.
Notice that, for this dataset, we used this [exact commit](https://github.com/toddwschneider/nyc-taxi-data/commit/c65ad8332a44f49770644b11576c0529b40bbc76).

### Requirements
To generate the data, you need PostgreSQL, PostGIS, and R.
For R, you will also need the following libraries: arrow, tidyverse, and glue. R is mostly used to convert the Parquet files into CSV files so that PostgreSQL can successfully load them.

### Download Files
To download the data, simply execute:
```bash
./download_raw_data.sh
```

### Ingesting the Data into PostgreSQL
For this benchmark, there are 3 scripts that must be run:
* `./initialize_database.sh` initializes the PostgreSQL instance and loads the weather dataset.
* `./import_yellow_taxi_trip_data.sh` converts the yellow taxi dataset files and ingests them.
* `./import_green_taxi_trip_data.sh` converts the green taxi dataset files and ingests them.

### Generating the CSV File
The following query is used to generate the CSV file. Note that the query is based on the one described in the [Billion NYC Taxi Rides Redshift](https://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html) blog post. Minor column name adjustments were made, as some names have changed since the original blog post was written (e.g., `weather.precipitation rain` was originally named `weather.precipitation_tenths_of_mm rain`).

```sql
COPY (
   SELECT trips.id,
           trips.vendor_id,
           trips.pickup_datetime,
           trips.dropoff_datetime,
           trips.store_and_fwd_flag,
           trips.rate_code_id,
           trips.pickup_longitude,
           trips.pickup_latitude,
           trips.dropoff_longitude,
           trips.dropoff_latitude,
           trips.passenger_count,
           trips.trip_distance,
           trips.fare_amount,
           trips.extra,
           trips.mta_tax,
           trips.tip_amount,
           trips.tolls_amount,
           trips.ehail_fee,
           trips.improvement_surcharge,
           trips.total_amount,
           trips.payment_type,
           trips.trip_type,
           trips.pickup_location_id,
           trips.dropoff_location_id,
           cab_types.type cab_type,
           weather.precipitation rain,
           weather.snow_depth,
           weather.snowfall,
           weather.max_temperature max_temp,
           weather.min_temperature min_temp,
           weather.average_wind_speed wind,
           pick_up.gid pickup_nyct2010_gid,
           pick_up.ctlabel pickup_ctlabel,
           pick_up.borocode pickup_borocode,
           pick_up.boroname pickup_boroname,
           pick_up.ct2010 pickup_ct2010,
           pick_up.boroct2010 pickup_boroct2010,
           pick_up.cdeligibil pickup_cdeligibil,
           pick_up.ntacode pickup_ntacode,
           pick_up.ntaname pickup_ntaname,
           pick_up.puma pickup_puma,
           drop_off.gid dropoff_nyct2010_gid,
           drop_off.ctlabel dropoff_ctlabel,
           drop_off.borocode dropoff_borocode,
           drop_off.boroname dropoff_boroname,
           drop_off.ct2010 dropoff_ct2010,
           drop_off.boroct2010 dropoff_boroct2010,
           drop_off.cdeligibil dropoff_cdeligibil,
           drop_off.ntacode dropoff_ntacode,
           drop_off.ntaname dropoff_ntaname,
           drop_off.puma dropoff_puma
    FROM trips
    LEFT JOIN cab_types
        ON trips.cab_type_id = cab_types.id
    LEFT JOIN central_park_weather_observations weather
        ON weather.date = trips.pickup_datetime::date
    LEFT JOIN nyct2010 pick_up
        ON pick_up.gid = trips.pickup_nyct2010_gid
    LEFT JOIN nyct2010 drop_off
        ON drop_off.gid = trips.dropoff_nyct2010_gid
) TO './output.csv' WITH CSV;
```

After generating the file, all that remains to be done is to split it and gzip the result. This can be achieved using the commands:
```bash
split -l 20000000 output.csv split/trips_
gzip trips_*
```
