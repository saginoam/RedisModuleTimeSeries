import redis
import json

client = redis.Redis()

client.execute_command('TS.CREATEDOC', "tsdoctest", json.dumps({
        "interval": "hour",
        "timestamp": "2016:01:01 00:00:00",
        "key_fields": ["userId", "deviceId"],
        "ts_fields": ["pagesVisited", "storageUsed", "trafficUsed"]
    }))

client.execute_command('TS.INSERTDOC', "tsdoctest", json.dumps({
        "userId": "uid1",
        "deviceId": "devid1",
        "pagesVisited": 1,
        "storageUsed": 2,
        "trafficUsed": 3,
        "timesamp": "2016:01:01 00:01:00"
    }))
