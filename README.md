# RedisModuleTimeSeries

###Time series values aggregation using redis modules api

# Overview

RedisModuleTimeSeries implements time series data aggregation in redis.
The api accepts json data and aggregate values into set of keys divided into date time histogram.
Quering the values is done using the standard redis api.

# API

The time series contains 2 apis: configuration and data.

## TS.conf

Create the configration for a specific time series aggregation

### Parameters

* name - the name of the time series
* conf - json string containing time series configuration. conf parameters:
  * keep_original: Boolean - Indicates to store the original data added in addition to aggregated data. (Not supported yet).
  * interval: String - The time interval for the aggregation. Values: second, minute, hour, day, month, year.
  * timeformat: String - Format of timestamp in data. If this field is missing, the timestamp from the data is ignored and 'now' is used. 
  * key_fields: Array of strings containing the data fields that will be used to create the time series key.
  * ts_fields: Array of objects. Object fields:
    * field: String - name of parameter to aggregate.
    * aggregation: String - Aggregation time. Values: sum, avg.
  

## TS.add

### Parameters

* name - the name of the time series
* data - json string containing time series data. data parameters:
  * All fields configured in TS.Conf command
  * timestamp: String - timestamp of aggregated data. If missing, 'now' is used. If it exists, it must be in the format specified in timeformat parameter configured in ts.conf.


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

#### Example

redis-cli is used in these example.
redis-cli doesn't support single quate ('), so all json quates must be escaped. 

* TS.CONF
```
127.0.0.1:6379> ts.conf testts "{\"keep_original\":false,\"interval\":\"day\",\"timeformat\":\"%Y:%m:%d %H:%M:%S\",\"key_fields\":[\"userId\",\"accountId\"],\"ts_fields\":[{\"field\":\"pagesVisited\",\"aggregation\":\"sum\"},{\"field\":\"storageUsed\",\"aggregation\":\"sum\"},{\"field\":\"trafficUsed\",\"aggregation\":\"avg\"}]}"
```

* TS.ADD
```
127.0.0.1:6379> ts.add testts "{\"userId\":\"userId1\",\"accountId\":\"accountId1\",\"pagesVisited\":2.500000,\"storageUsed\":111,\"trafficUsed\":20,\"timestamp\":\"2016:10:05 06:40:01\"}"
```

## TODO

 * Expiration for aggregated data
 * interval duration. i.e '10 minute'
 * Implement 'keep original'
 * Interval should be per field, not global
 * Compare performance and footprint with elasticsearch (and others)
 * Additional tests
 * API to get the aggregated data. Its not really needed since all data can be retreived using redis apis.