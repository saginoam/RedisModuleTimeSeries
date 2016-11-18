import redis

tskey = "pyts"
client = redis.Redis()

ret = client.execute_command('DEL', tskey)
print ret

ret = client.execute_command('TS.CREATE', tskey, "day", "2016:01:01 00:00:00")
print ret

ret = client.execute_command('TS.INSERT', tskey, "10.5", "2016:01:02 00:00:00")
print ret

ret = client.execute_command('TS.INSERT', tskey, "11.5", "2016:01:02 00:01:00")
print ret

ret = client.execute_command('TS.GET', tskey, "sum", "2016:01:02 00:01:00")
print ret[0]
assert (ret[0] == '22')

ret = client.execute_command('TS.GET', tskey, "avg", "2016:01:02 00:01:00")
print ret[0]
assert (ret[0] == '11')
