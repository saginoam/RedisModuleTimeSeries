import random

import redis
import json

client = redis.Redis()

client.execute_command('TS.CREATEDOC', "tsdoctest", json.dumps({
        "interval": "hour",
        "timestamp": "2016:01:01 00:00:00",
        "key_fields": ["userId", "deviceId"],
        "ts_fields": ["pagesVisited", "storageUsed", "trafficUsed"]
    }))

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
