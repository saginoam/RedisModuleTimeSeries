from datetime import datetime
from elasticsearch import Elasticsearch
import redis

tskey = "pytsbench"
es = Elasticsearch()
client = redis.Redis()

def info():
    '''
    Data:
    storage_used
    pages_visited
    usage_time

    unique ids:
    user_id
    device_id

    Metadata:
    username
    email
    account
    '''

def add_redis_entry(i, day, hour):
    timestamp = "2016:01:%.2d %.2d:00:00" % (day, hour)
    user_id = "user_id_%d" % (i)
    dev_id = "device_id_%d" % (i)
    key = "%s_%s_%s" % (tskey, user_id, dev_id)
    if day == 1 and hour == 0:
        client.hmset(key, {
            "user_id": user_id,
            "device_id": dev_id,
            "username": "username%d" % (i),
            "email": "username%d@timeseries.com" % (i),
            "account": "standard"
        })
        client.execute_command('TS.CREATE', key + "_storage_used", "day", "2016:01:01 00:00:00")
        client.execute_command('TS.CREATE', key + "_pages_visited", "day", "2016:01:01 00:00:00")
        client.execute_command('TS.CREATE', key + "_usage_time", "day", "2016:01:01 00:00:00")

    client.execute_command('TS.INSERT', key + "_storage_used", str(i * 1.1), timestamp)
    client.execute_command('TS.INSERT', key + "_pages_visited", str(i), timestamp)
    client.execute_command('TS.INSERT', key + "_usage_time", str(i * 0.2), timestamp)



def add_es_entry(i, day, hour):
    timestamp = "2016:01:%.2d %.2d:00:00" % (day, hour)
    user_id = "user_id_%d" % (i)
    dev_id = "device_id_%d" % (i)
    key = "%s_%s" % (user_id, dev_id)
    script = "ctx._source.count += 1; "
    script += "ctx._source.storage_used += storage_used; "
    script += "ctx._source.pages_visited += pages_visited; "
    script += "ctx._source.usage_time += usage_time; "
    es.update(index=tskey, doc_type=tskey, id=key+"_"+timestamp, body={
        "script": script,
        "params": {
            "storage_used": i * 1.1,
            "pages_visited": i,
            "usage_time": i * 0.2
        },
        "upsert": {
            "storage_used": i * 1.1,
            "pages_visited": i,
            "usage_time": i * 0.2,
            "user_id": user_id,
            "device_id": dev_id,
            "username": "username%d" % (i),
            "email": "username%d@timeseries.com" % (i),
            "account": "standard",
            "count": 1
        }
    })


def add_entry(i, day, hour):
    add_redis_entry(i, day, hour)
    #add_es_entry(i, day, hour)

#add_redis_entry(2, 30, 1)
start = datetime.now()
for i in range(1, 100):
    for day in range(1, 31):
        for hour in range(0, 24):
            add_entry(i, day, hour)
end = datetime.now()
print(end-start)