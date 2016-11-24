from datetime import datetime
from elasticsearch import Elasticsearch
import redis

tskey = "pytsbench"
es = Elasticsearch(["localhost:9200"])
elastic_5_0 = True
client = redis.Redis()

num_entries = 3

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


def delete_all():
    try:
        es.indices.delete(tskey)
    except:
        pass
    for key in client.execute_command('KEYS', tskey + "*"):
        client.execute_command('DEL', key)

def get_timestamp(day, hour, minute):
    return "2016:01:%.2d %.2d:%.2d:00" % (day, hour, minute)

def add_redis_entry(i, day, hour):
    timestamp = get_timestamp(1, 0, 0)
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
        client.execute_command('TS.CREATE', key + "_storage_used", "hour", timestamp)
        client.execute_command('TS.CREATE', key + "_pages_visited", "hour", timestamp)
        client.execute_command('TS.CREATE', key + "_usage_time", "hour", timestamp)

    for e in range(1, num_entries + 1):
        timestamp = get_timestamp(day, hour, e)
        client.execute_command('TS.INSERT', key + "_storage_used", str(i * 1.1 * e), timestamp)
        client.execute_command('TS.INSERT', key + "_pages_visited", str(i * e), timestamp)
        client.execute_command('TS.INSERT', key + "_usage_time", str(i * 0.2 * e), timestamp)


def add_es_entry(i, day, hour):
    timestamp = get_timestamp(day, hour, 0)
    prefix = "params." if elastic_5_0 else ""
    user_id = "user_id_%d" % (i)
    dev_id = "device_id_%d" % (i)
    key = "%s_%s" % (user_id, dev_id)
    script = "ctx._source.count += 1; "
    script += "ctx._source.storage_used += %sstorage_used; " % (prefix)
    script += "ctx._source.pages_visited += %spages_visited; "  % (prefix)
    script += "ctx._source.usage_time += %susage_time; "  % (prefix)

    # Can't really perform the aggregation in elastic. its too slow.
    #for e in range(1, num_entries + 1):
    for e in range(1, 2):
        params = {
            "storage_used": i * 1.1 * e,
            "pages_visited": i * e,
            "usage_time": i * 0.2 * e
        }
        upsert = {
            "user_id": user_id,
            "device_id": dev_id,
            "username": "username%d" % (i),
            "email": "username%d@timeseries.com" % (i),
            "account": "standard",
            "count": 1
        }
        upsert.update(params)
        script_1_7 = {
            "script": script,
            "params": params,
            "upsert": upsert
        }
        script_5_0 = {
            "script": {
                "inline": script,
                "lang": "painless",
                "params": params
            },
            "upsert": upsert
        }
        body = script_5_0 if elastic_5_0 else script_1_7
        es.update(index=tskey, doc_type=tskey, id=key + "_" + timestamp, body=body)

def add_es_entry_5_0(i, day, hour):
    add_es_entry(i, day, hour, True)


def sizeof_fmt(num, suffix='b'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def get_redis_size(thekey = tskey):
    redis_size = 0
    for key in client.execute_command('KEYS', thekey + "*"):
        redis_size += client.execute_command('DEBUG OBJECT', key)['serializedlength']
    return "size: %s (%d)" % (sizeof_fmt(redis_size), redis_size)

def get_es_size():
    ind = 0
    try:
        ret = es.indices.stats(tskey)
        ind = ret['_all']['total']['store']['size_in_bytes']
    except:
        pass
    return "size: %s (%d)" % (sizeof_fmt(ind), ind)

def run_for_all(size, cb, msg, size_cb):
    start = datetime.now().replace(microsecond=0)
    for i in range(1, size + 1):
        for day in range(1, 31):
            for hour in range(0, 24):
                cb(i, day, hour)
    end = datetime.now().replace(microsecond=0)
    print msg, (end - start), size_cb()


def do_benchmark(size):
    print "delete data"
    delete_all()
    print "----------------------------------------"
    print "benchmark size: ", size, "number of calls: ", num_entries * size * 24 * 30
    run_for_all(size, add_redis_entry,  "redis  ", get_redis_size)
    #run_for_all(size, add_redis_hset_entry,  "hset ", get_redis_hset_size)
    #run_for_all(size, add_redis_list_entry,  "list ", get_redis_list_size)
    run_for_all(size, add_es_entry, "elastic ", get_es_size)
    #run_for_all(size, add_es_entry_5_0, "es5_0", get_es_size_5_0)
    print "----------------------------------------"


do_benchmark(1)
do_benchmark(10)
#do_benchmark(100)
#do_benchmark(1000)

# TODO benchmark get api
# TODO Use bulk api
