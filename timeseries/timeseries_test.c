#include "timeseries.h"

#define TEST_CONF \
  "{ \
    \"keep_original\": false, \
    \"key_fields\": [ \"f1\", \"f2\" ], \
    \"ts_fields\": [ \
      { \
       \"field\":\"s1\", \
       \"aggregation\": \"sum\" \
      }, \
      { \
       \"field\":\"s2\", \
       \"aggregation\": \"sum\" \
      }, \
      { \
       \"field\":\"a1\", \
       \"aggregation\": \"avg\" \
      } \
     ], \
    \"interval\": \"DAY\" \
   }"

#define TEST_DATA \
  "{ \
    \"f1\": \"fff1\", \
    \"f2\": \"fff2\", \
    \"s1\": 10.5, \
    \"s2\": 111, \
    \"a1\": 10 \
   }"

#define TEST_DATA2 \
  "{ \
    \"f1\": \"fff1\", \
    \"f2\": \"fff2\", \
    \"s1\": 2.5, \
    \"s2\": 111, \
    \"a1\": 20 \
   }"

// TODO Verify all keys exist
// TODO Verify all fields exist
// TODO Verify single entry sum
// TODO Verify single entry avg
// TODO Verify multiple entry sum
// TODO Verify multiple entry avg
// TODO Verify keep original
// TODO Verify timestamp in data json

int testTS(RedisModuleCtx *ctx) {
  long count, timestamp = interval_timestamp("day");
  double val;
  char *eptr, timestamp_key[100], count_key[100], str[] = TEST_CONF;

  sprintf(timestamp_key, "%li", timestamp);
  sprintf(count_key, "%s:count", timestamp_key);

  // Verify invalid json in conf
  RedisModuleCallReply *r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", "this is not a json string");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json\r\n"));

  // Remove old data (previous tests)
  r = RedisModule_Call(ctx, "DEL", "c", "ts.conf");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  r = RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s2:sum");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  r = RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:a1:avg");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  r = RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s1:sum");
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // Validate aggregation type
  strncpy (strstr (str,"avg"),"xxx", 3);
  r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", str);
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json: aggregation is not one of: sum, avg\r\n"));
  strncpy (strstr (str,"xxx"),"avg", 3);

  // Validate interval values
  strncpy (strstr (str,"DAY"),"xxx", 3);
  r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", str);
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL),
    "Invalid json: interval is not one of: second, minute, hour, day, week, month, year\r\n"));
  strncpy (strstr (str,"xxx"),"DAY", 3);

  // Validate add before conf fails
  r = RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA);
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);

  // Add conf
  r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", TEST_CONF);
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // 1st Add succeed
  r = RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA);
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // Verify count is 1
  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", count_key);
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 1);

  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", count_key);
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 1);

  // Verify value
  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", timestamp_key);
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 10.5);

  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", timestamp_key);
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 10);

  // 2nd Add
  r = RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA2);
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // Verify count is 2
  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", count_key);
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 2);

  // Verify sum value is aggregated
  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", timestamp_key);
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 13);

  r = RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", timestamp_key);
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 15);

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
