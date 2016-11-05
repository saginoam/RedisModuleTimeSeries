#include "timeseries.h"

// TODO Verify all keys exist
// TODO Verify all fields exist
// TODO Verify keep original
// TODO Verify timestamp in data json

cJSON *ts_object(char *field, char *agg) {
  cJSON *ts = cJSON_CreateObject();
  cJSON_AddStringToObject(ts, "field", field);
  cJSON_AddStringToObject(ts, "aggregation", agg);
  return ts;
}

cJSON *testConfBase(cJSON *conf, char *avg, char *interval) {
  if (conf)
    cJSON_Delete(conf);

  const char *key_fields[]={"f1","f2"};
  cJSON *root = cJSON_CreateObject();
  cJSON_AddFalseToObject(root, "keep_original");
  cJSON_AddStringToObject(root, "interval", interval);
  cJSON_AddItemToObject(root, "key_fields", cJSON_CreateStringArray(key_fields, 2));
  cJSON *ts_fields = cJSON_CreateArray();
  cJSON_AddItemToArray(ts_fields, ts_object("s1", SUM));
  cJSON_AddItemToArray(ts_fields, ts_object("s2", SUM));
  cJSON_AddItemToArray(ts_fields, ts_object("a1", avg));
  cJSON_AddItemToObject(root, "ts_fields", ts_fields);
  return root;
}

cJSON *testConf(cJSON *conf) {
  return testConfBase(conf, AVG, DAY);
}

cJSON *testConfInvalidAvg(cJSON *conf) {
  return testConfBase(conf, "xxx", DAY);
}

cJSON *testConfInvalidInterval(cJSON *conf) {
  return testConfBase(conf, AVG, "xxx");
}

cJSON *dataJson(double s, double a) {
  cJSON *data = cJSON_CreateObject();
  cJSON_AddStringToObject(data, "f1", "fff1");
  cJSON_AddStringToObject(data, "f2", "fff2");
  cJSON_AddNumberToObject(data, "s1", s);
  cJSON_AddNumberToObject(data, "s2", 111);
  cJSON_AddNumberToObject(data, "a1", a);
  return data;
}

int testTS(RedisModuleCtx *ctx) {
  long count, timestamp = interval_timestamp(DAY, NULL, NULL);
  double val;
  char *eptr, timestamp_key[100], count_key[100];

  sprintf(timestamp_key, "%li", timestamp);
  sprintf(count_key, "%s:count", timestamp_key);

  RedisModuleCallReply *r = NULL;
  cJSON *confJson = NULL, *data1 = dataJson(10.5, 10), *data2 = dataJson(2.5, 20);

  // Verify invalid json in conf
  RMCALL_Assert(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", "this is not a json string"),
    RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json\r\n"));

  // Remove old data (previous tests)
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "DEL", "c", "ts.conf"));
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s2:sum"));
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:a1:avg"));
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s1:sum"));

  // Validate aggregation type
  confJson = testConfInvalidAvg(confJson);
  RMCALL(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", cJSON_Print_static(confJson)));
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json: aggregation is not one of: sum, avg\r\n"));

  // Validate interval values
  confJson = testConfInvalidInterval(confJson);
  RMCALL(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", cJSON_Print_static(confJson)));
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL),
    "Invalid json: interval is not one of: second, minute, hour, day, week, month, year\r\n"));

  // Validate add before conf fails
  RMCALL(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", cJSON_Print_static(data1)));
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);

  // Add conf
  confJson = testConf(confJson);
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", cJSON_Print_static(confJson)));

  // 1st Add succeed
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", cJSON_Print_static(data1)));

  // Verify count is 1
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", count_key));
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 1);

  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", count_key));
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 1);

  // Verify value
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 10.5);

  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 10);

  // 2nd Add
  RMCALL_AssertNoErr(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", cJSON_Print_static(data2)));

  // Verify count is 2
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", count_key));
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 2);

  // Verify sum value is aggregated
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 13);

  // Verify avg value is aggregated
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 15);

  cJSON_Delete(data1);
  cJSON_Delete(data2);
  cJSON_Delete(confJson);
  RedisModule_FreeCallReply(r);
  return 0;
}

// Unit test entry point for the timeseries module
int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  RMUtil_Test(testTS);
  // Run the test twice. Make sure no leftovers in either ts or test.
  RMUtil_Test(testTS);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}
