# RedisModuleTimeSeries

###Time series values aggregation using redis modules api

# Overview

RedisModuleTimeSeries implements time series data aggregation in redis.
The module provides two variants APIs: single aggregation and json document. 
The json document API is used in order to stream reports into redis, and let
redis perform the ETL on the report, thus allowing the streaming pipeline to be 
agnostic to the data itself.
 
# API

## TS.CREATE

Creates a time series key


##ts.insert

Insert time series data to be aggregated

##ts.get

Query the aggregated key for statistics (sum, avg, count)

##ts.info

Get information on a time series key

##ts.createdoc

Create a json document configuration

##ts.insertdoc

Insert a json document to be converted to time series data.

## Building and running:


#### Prerequisites

To run the modules, a redis server that supports modules is required.
In case there is no redis server, or the redis server does not support modules, get latest redis:

```sh
git clone https://github.com/antirez/redis.git
cd redis
make

# In case compatibility is broken, use this fork  (https://github.com/saginoam/redis.git)
```

#### Build


```sh
git clone https://github.com/saginoam/RedisModuleTimeSeries.git
cd RedisModuleTimeSeries
make
```

#### Run
```sh
/path/to/redis-server --loadmodule ./timeseries/timeseries.so
```

#### Examples



## TODO

 * Expiration for aggregated data
 * Interval duration. i.e '10 minutes'
 * Additional analytics APIs 
 * Store metadata for time series key
 
# Benchmark

TBD
