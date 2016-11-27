import redis
import json

def client():
    return redis.Redis()

print int(client().info("memory")['used_memory'])

tskey = "tsdoctest"

def get_timestamp(day=0, hour=0, minute=0):
    return "2016:01:%.2d %.2d:%.2d:00" % (day, hour, minute)

def doc_cofig():
    return json.dumps({
        "interval": "hour",
        "timestamp": get_timestamp(),
        "key_fields": ["userId", "deviceId"],
        "ts_fields": ["pagesVisited", "storageUsed", "trafficUsed"]
    })

def doc():
    return json.dumps({
        "userId": "uid1",
        "deviceId": "devid1",
        "pagesVisited": 1,
        "storageUsed": 2,
        "trafficUsed": 3,
        "timesamp": get_timestamp()
    })


print client().execute_command('DEL', tskey)

# print json.dumpsdoc_cofig()
print client().execute_command('TS.CREATEDOC', tskey, doc_cofig())

print client().execute_command('TS.INSERTDOC', tskey, doc())
print client().execute_command('TS.INSERTDOC', tskey, doc())
print client().execute_command('TS.INSERTDOC', tskey, doc())
#print client.execute_command('TS.INSERTDOC', tskey, doc())

print client().execute_command('KEYS', tskey + "*")
print client().keys(tskey + "*")
print int(client().info("memory")['used_memory'])
# print ret[0]
# assert (ret[0] == '22')
#
# ret = client.execute_command('TS.GET', tskey, "avg", "2016:01:02 00:01:00")
# print ret[0]
# assert (ret[0] == '11')
