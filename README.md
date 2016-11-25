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
considering memory usage and speed of the insert/upsert operations.
 
Redis is excellent for this use case. Especially with the new module API, allowing much of the work to be done in redis.

Benchmark results included.

# API

## TS.CREATE

Creates a time series key. Must be called before the call to TS.INSERT on that key. 

### Parameters

* name - Name of the key
* Interval - The time interval for data aggregation. Allowed values: second, minute, hour, day, month, year.
  For example, if the inetrval is hour, all values that are inserted between 09:00-10:00 are added to the same aggregation.
* init_timestamp - (Optional) The earliest time that values can be added. Default is now.
  It is used for aggregating old data that was not originally streamed into redis.
  Currently the only supported time format is: "%Y:%m:%d %H:%M:%S". A configurable time format is in roadmap.

##TS.INSERT

Insert time series data to be aggregated.

### Parameters

* name - Name of the key
* value - the value to add to time series aggregation.
* timestamp - (Optional) The time that value was added. Default is now.
  It is used for aggregating old data that was not originally streamed into redis. 


##TS.GET

Query the aggregated key for statistics.

### Parameters

* name - Name of the key
* operation - The calculation to perform. Allowed values: sum, avg, count.
* start_time - (Optional) The start time for the aggregation. Default is now.
* end_time - (Optional) The end time for the aggregation. Default is now.

##TS.INFO

Get information on a time series key. Returns init timestamp, last timestamp, length and interval.

### Parameters

* name - Name of the key

##TS.CREATEDOC

Create a json document configuration. This is used in order to insert a json document into redis and let redis extract
the information from the document and convert it into a set of TS.INSERT commands on the keys and values extracted from
the document. Must be called before the call to TS.INSERTDOC on that key.

### Parameters

* name - Name of the document
* json - A json containing the information required in order to extract data from documents that are inserted using the 
  TS.INSERTDOC command. The json contains the following fields:
  * key_fields - list of field names that will be used to create the aggregated key.
  * ts_fields - list of field names to perform aggregation on.
  * interval - The time interval for data aggregation. Allowed values: second, minute, hour, day, month, year.
  * timestamp - (Optional) The earliest time that values can be added. Default is now.

##TS.INSERTDOC

Insert a json document to be converted to time series data.

### Parameters

* name - Name of the document
* json - A json containing the data to aggregate. The json document must contain all the fields that exist in the
  'key_fields' and 'ts_fields' configured in TS.CREATEDOC.

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

TBD

## TODO

 * Expiration for aggregated data
 * Interval duration. i.e '10 minutes'
 * Additional analytics APIs 
 * key Metadata. The meta data is inserted only in the creation of the key and
   used later for filtering/grouping by the analytics api.
 * Time series metadata. Meta data that is updated on each aggregation (for example kafka offset)
   and can be used by the client application to ensure each document is counted 'exactly once'. 
 * Configurable time format.
 
# Benchmark

TBD
