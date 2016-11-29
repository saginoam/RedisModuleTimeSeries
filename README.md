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

## Examples

The examples of the basic API are done using redis-cli.
The examples of the json documents aggregation are done using python code.

### TS.CREATE

Create time series key with hour interval

```
127.0.0.1:6379> TS.CREATE testaggregation hour
OK
```

###TS.INSERT

Insert some values to time series key

```
127.0.0.1:6379> TS.INSERT testaggregation 10.5
OK
```

```
127.0.0.1:6379> TS.INSERT testaggregation 11.5
OK
```

###TS.GET

Get aggregation values for that key.

```
127.0.0.1:6379> TS.GET testaggregation sum
1) "22"
```

```
127.0.0.1:6379> TS.GET testaggregation avg
1) "11"
```

```
127.0.0.1:6379> TS.GET testaggregation count
1) (integer) 2
```

###TS.INFO

Get information on that timeseries

```
127.0.0.1:6379> TS.INFO testaggregation
"Start: 2016:11:26 19:00:00 End: 2016:11:26 19:00:00 len: 1 Interval: hour"
```

###TS.CREATEDOC

Create json doc configuration, to enable ingestion of reports into redis.

```
import redis
import json

client = redis.Redis()

client.execute_command('TS.CREATEDOC', "tsdoctest", json.dumps({
        "interval": "hour",
        "timestamp": "2016:01:01 00:00:00",
        "key_fields": ["userId", "deviceId"],
        "ts_fields": ["pagesVisited", "storageUsed", "trafficUsed"]
    }))
```

###TS.INSERTDOC

Insert report to redis time series module. Based on the configuration in TS.CREATEDOC

```
import redis
import json

client = redis.Redis()

client.execute_command('TS.INSERTDOC', "tsdoctest", json.dumps({
        "userId": "uid1",
        "deviceId": "devid1",
        "pagesVisited": 1,
        "storageUsed": 2,
        "trafficUsed": 3,
        "timesamp": "2016:01:01 00:01:00"
    }))

```

## Data Stream example

The power if this module can be demonstrated by simulating a data stream.
Data stream can be, for example, a report collecting system that stores incoming reports into kafka and loads them into redis using logstash.
In that case the data time series aggregation can be acheived with nothing more than logstash configuration without any code.

#### Create time series document
Run the following python code to configure the time series aggregation
```
import redis
import json

client = redis.Redis()

client.execute_command('TS.CREATEDOC', "tsdoctest", json.dumps({
        "interval": "hour",
        "timestamp": "2016:01:01 00:00:00",
        "key_fields": ["userId", "deviceId"],
        "ts_fields": ["pagesVisited", "storageUsed", "trafficUsed"]
    }))

```

#### Stream documents into redis

Run the bellow code to stream documents into redis.
Streaming documents into redis can be done for example using kafka and logstash.
For the simplicity of the demo I use python code

```
import random
import redis
import json

client = redis.Redis()

def doc(userId, deviceId, hour, minute):
    return json.dumps({
        "userId": userId,
        "deviceId": deviceId,
        "pagesVisited": random.randint(1, 10),
        "storageUsed": random.randint(1, 10),
        "trafficUsed": random.randint(1, 10),
        "timestamp": "2016:01:01 %.2d:%.2d:00" % (hour, minute)
    })

# Simulate a data stream such as logstash with input kafka output redis
for hour in range(0, 24):
    for minute in range(0, 60, 5):
        client.execute_command('TS.INSERTDOC', "tsdoctest", doc("user1", "deviceA", hour, minute))
        client.execute_command('TS.INSERTDOC', "tsdoctest", doc("user1", "deviceA", hour, minute))
        client.execute_command('TS.INSERTDOC', "tsdoctest", doc("user1", "deviceB", hour, minute))
        client.execute_command('TS.INSERTDOC', "tsdoctest", doc("user2", "deviceB", hour, minute))
        client.execute_command('TS.INSERTDOC', "tsdoctest", doc("user2", "deviceC", hour, minute))

```

#### Query redis for time series aggregation

See what keys are created
```
127.0.0.1:6379> keys *
 1) "tsdoctest:user2:deviceC:storageUsed"
 2) "tsdoctest:user1:deviceA:trafficUsed"
 3) "tsdoctest:user1:deviceA:storageUsed"
 4) "tsdoctest:user2:deviceB:pagesVisited"
 5) "tsdoctest:user2:deviceB:storageUsed"
 6) "tsdoctest:user1:deviceA:pagesVisited"
 7) "tsdoctest"
 8) "tsdoctest:user2:deviceC:pagesVisited"
 9) "tsdoctest:user1:deviceB:pagesVisited"
10) "tsdoctest:user2:deviceC:trafficUsed"
11) "tsdoctest:user1:deviceB:storageUsed"
12) "tsdoctest:user1:deviceB:trafficUsed"
13) "tsdoctest:user2:deviceB:trafficUsed"
```

Get information on specific key
```
127.0.0.1:6379> TS.INFO tsdoctest:user2:deviceC:trafficUsed
"Start: 2016:01:01 00:00:00 End: 2016:01:01 23:00:00 len: 24 Interval: hour"
```

Query a specific key for avg in a single timestamp 
```
127.0.0.1:6379> TS.GET tsdoctest:user2:deviceC:trafficUsed avg "2016:01:01 00:00:00"
1) "6.5"
```

Query a specific key for avg in a time range
```
127.0.0.1:6379> TS.GET tsdoctest:user2:deviceC:trafficUsed avg "2016:01:01 00:00:00" "2016:01:01 03:00:00"
1) "6.5"
2) "4.75"
3) "5.666666666666667"
4) "5.75"
```

Query a specific key for documents count in a time range
```
127.0.0.1:6379> TS.GET tsdoctest:user2:deviceC:trafficUsed count "2016:01:01 00:00:00" "2016:01:01 03:00:00"
1) (integer) 12
2) (integer) 12
3) (integer) 12
4) (integer) 12
```

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

I performed the benchmark with many configuration. Used elastic 1.7 and 5.0,
performed single api call and bulk api calls, used both groovy and painless for elasticsearch upsert.
   
Insert performance: redis 10x times faster than elasticsearch.

Upsert performance: redis 10-100x times faster than elasticsearch.

Memory usage: When elasticsearch was configured with '_source: disable', the memory usage was similar.
When '_source: enable', the elasticsearch memory usage increased drastically (3-10 times higher).
