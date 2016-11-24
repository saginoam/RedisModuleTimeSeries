# RedisModuleTimeSeries

###Time series values aggregation using redis modules api

# Overview

RedisModuleTimeSeries implements time series data aggregation in redis.
The module provides two variants APIs: single aggregation and json document. 
The json document API is used in order to stream reports into redis, and let
redis perform the ETL on the report, thus allowing the streaming pipeline to be 
agnostic to the data itself.
 
The motivation for this project arrived from what I consider a misuse of other
frameworks, mainly elasticsearch, for doing time series aggregation on structured numerical data.

Elasticseach is a json based search engine with analytics APIs and many other features.
I consider the ELK to be state of the art framework, Its best in its domain, which is collecting, analyzing and visualize
unstructured textual data, such as application logs, email analyzing, user tweets, etc.

But, for collecting and analyzing structured data, which contains mostly predefined numerical
parameters it is not the preferred solution, IMO, and thats what I'm trying to prove here.   

For such use case I would use a software that allows me to store and analyze the data in the most efficient way,
considering memory usage, speed and low cost (virtual) hardware requirements.
 
Redis is excellent for this use case. Especially with the new module API, allowing much of the work to be done in redis.

Benchmark results included.


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

The results are really amazing, and they show what is cost of using the wrong tool.
For the same amount of reports, redis was more than 10x time faster and used 1% of the
memory that elastic search used.

Its not really a surprise since elastic stores all the data as json. Also, in elastic we
store all the meta data in each report. You can argue its wrong usage, but I was just using
in this benchmark the same procedures I argue that they are wrong. Also, if we would have removed
the metadata from the reports, Redis would still use only 3% (instead of 1%) of the memory used by elastic.
  
Here is the output of the benchmark (performed using the benchmark.py file):
```
benchmark size:  1 number of calls:  2160
redis    0:00:00 size: 458.0 b (458 bytes)
elastic  0:00:07 size: 46.3 Kb (47394 bytes)
----------------------------------------
benchmark size:  10 number of calls:  21600
redis    0:00:04 size:   4.6 Kb (4755 bytes)
elastic  0:00:37 size: 594.8 Kb (609093 bytes)

```

