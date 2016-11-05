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
    \"interval\": \"day\" \
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
  long count, timestamp = interval_timestamp("day", NULL, NULL);
  double val;
  char *eptr, timestamp_key[100], count_key[100], str[] = TEST_CONF;

  sprintf(timestamp_key, "%li", timestamp);
  sprintf(count_key, "%s:count", timestamp_key);

  // Verify invalid json in conf
  RedisModuleCallReply *r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", "this is not a json string");
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json\r\n"));

  // Remove old data (previous tests)
  RMCALL(r, RedisModule_Call(ctx, "DEL", "c", "ts.conf"));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  RMCALL(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s2:sum"));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  RMCALL(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:a1:avg"));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  RMCALL(r, RedisModule_Call(ctx, "DEL", "c", "ts.agg:fff1:fff2:s1:sum"));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);
  RedisModule_FreeCallReply(r);

  // Validate aggregation type
  strncpy (strstr (str,"avg"),"xxx", 3);
  r = RedisModule_Call(ctx, "ts.conf", "cc", "testts", str);
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL), "Invalid json: aggregation is not one of: sum, avg\r\n"));
  strncpy (strstr (str,"xxx"),"avg", 3);

  // Validate interval values
  strncpy (strstr (str,"day"),"xxx", 3);
  RMCALL(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", str));
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);
  RMUtil_Assert(!strcmp(RedisModule_CallReplyStringPtr(r, NULL),
    "Invalid json: interval is not one of: second, minute, hour, day, week, month, year\r\n"));
  strncpy (strstr (str,"xxx"),"day", 3);

  // Validate add before conf fails
  RMCALL(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA));
  RMUtil_Assert(RedisModule_CallReplyType(r) == REDISMODULE_REPLY_ERROR);

  // Add conf
  RMCALL(r, RedisModule_Call(ctx, "ts.conf", "cc", "testts", TEST_CONF));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // 1st Add succeed
  RMCALL(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

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
  RMCALL(r, RedisModule_Call(ctx, "ts.add", "cc", "testts", TEST_DATA2));
  RMUtil_Assert(RedisModule_CallReplyType(r) != REDISMODULE_REPLY_ERROR);

  // Verify count is 2
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", count_key));
  count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);
  RMUtil_Assert(count == 2);

  // Verify sum value is aggregated
  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:s1:sum", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 13);

  RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", "ts.agg:fff1:fff2:a1:avg", timestamp_key));
  val = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
  RMUtil_Assert(val == 15);

  RedisModule_FreeCallReply(r);
  return 0;
}


int test2test(RedisModuleCtx *ctx) {
  struct tm tm;
  time_t timestamp = interval_timestamp("hour", NULL, NULL);
  char timestamp_key[100];

  time_t t = time(NULL);
  sprintf(timestamp_key, "%li", t);
  printf("T : %s\n", timestamp_key);

  timestamp = interval_timestamp("second", NULL, NULL);
  sprintf(timestamp_key, "%li", timestamp);
  printf("Tm: %s\n", timestamp_key);

  timestamp = interval_timestamp("second", "2016:11:04 05:31:02", "%Y:%m:%d %H:%M:%S");
  sprintf(timestamp_key, "%li", timestamp);
  printf("Ts: %s\n", timestamp_key);


  timestamp = interval_timestamp("second", "2016-11-04T05:55:09Z", "%Y-%m-%dT%H:%M:%S");
  sprintf(timestamp_key, "%li", timestamp);
  printf("Ts: %s\n", timestamp_key);

  timestamp = interval_timestamp("second", "2016-11-04T05:55:09.000Z", "%Y-%m-%dT%H:%M:%S");
  sprintf(timestamp_key, "%li", timestamp);
  printf("Ts: %s\n", timestamp_key);

  printf("-------------------------------------\n");


  timestamp = interval_timestamp("minute", NULL, NULL);
  sprintf(timestamp_key, "%li", timestamp);
  printf("Tm: %s\n", timestamp_key);

  timestamp = interval_timestamp("hour", NULL, NULL);
  sprintf(timestamp_key, "%li", timestamp);
  printf("Th: %s\n", timestamp_key);

  timestamp = interval_timestamp("day", NULL, NULL);
  sprintf(timestamp_key, "%li", timestamp);
  printf("Td: %s\n", timestamp_key);

  gmtime_r(&t, &tm);

  printf ("now tm_sec: %d\n", tm.tm_sec);
  printf ("now tm_min: %d\n", tm.tm_min);
  printf ("now tm_hour: %d\n", tm.tm_hour);
  printf ("now tm_mday: %d\n", tm.tm_mday);
  printf ("now tm_mon: %d\n", tm.tm_mon);
  printf ("now tm_year: %d\n", tm.tm_year);
  printf ("now tm_wday: %d\n", tm.tm_wday);
  printf ("now tm_yday: %d\n", tm.tm_yday);
  printf ("now tm_isdst: %d\n", tm.tm_isdst);


  sprintf(timestamp_key, "%li", timestamp);
  gmtime_r(&timestamp, &tm);

  printf ("\n");
  printf ("now tm_sec: %d\n", tm.tm_sec);
  printf ("now tm_min: %d\n", tm.tm_min);
  printf ("now tm_hour: %d\n", tm.tm_hour);
  printf ("now tm_mday: %d\n", tm.tm_mday);
  printf ("now tm_mon: %d\n", tm.tm_mon);
  printf ("now tm_year: %d\n", tm.tm_year);
  printf ("now tm_wday: %d\n", tm.tm_wday);
  printf ("now tm_yday: %d\n", tm.tm_yday);
  printf ("now tm_isdst: %d\n", tm.tm_isdst);



  printf("Ts: %s\n", timestamp_key);
  printf("Tl: %li\n", timestamp);
  printf("%d %d %d\n", tm.tm_year, tm.tm_mon, tm.tm_mday);

  timestamp = interval_timestamp("second", "2016:11:04 05:31:02", "%Y:%m:%d %H:%M:%S");

  sprintf(timestamp_key, "%li", timestamp);

  printf("Ts: %s\n", timestamp_key);
  printf("Tl: %li\n", timestamp);
  gmtime_r(&timestamp, &tm);
  printf("%d %d %d\n", tm.tm_year, tm.tm_mon, tm.tm_mday);

  timestamp = interval_timestamp("day", "2015:10:31 10:01:02.000", "%Y:%m:%d %H:%M:%S");

  sprintf(timestamp_key, "%li", timestamp);

  printf("Ts: %s\n", timestamp_key);
  printf("Tl: %li\n", timestamp);
  gmtime_r(&timestamp, &tm);
  printf("%d %d %d\n", tm.tm_year, tm.tm_mon, tm.tm_mday);

  timestamp = interval_timestamp("day", "2015:09:11 10:01:02.000", "%Y:%m:%d %H:%M:%S");

  sprintf(timestamp_key, "%li", timestamp);

  printf("Ts: %s\n", timestamp_key);
  printf("Tl: %li\n", timestamp);
  gmtime_r(&timestamp, &tm);
  printf("%d %d %d\n", tm.tm_year, tm.tm_mon, tm.tm_mday);

  return 0;
}

// Unit test entry point for the timeseries module
int TestModule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  RMUtil_Test(testTS);
  // Run the test twice. Make sure no leftovers in either ts or test.
  RMUtil_Test(testTS);

  RMUtil_Test(test2test);

  RedisModule_ReplyWithSimpleString(ctx, "PASS");
  return REDISMODULE_OK;
}
