#include <sys/time.h>
#include <time.h>
#include "../redismodule.h"
#include "../rmutil/util.h"
#include "../rmutil/strings.h"
#include "../rmutil/test_util.h"
#include "../cJSON/cJSON.h"

// TODO add expiration
// TODO add interval duration. i.e '10 minute'

#define SECOND "second"
#define MINUTE "minute"
#define HOUR "hour"
#define DAY "day"
#define WEEK "week"
#define MONTH "month"
#define YEAR "year"

char *validIntervals[] = {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR};

#define SUM "sum"
#define AVG "avg"

char *validAggs[] = {SUM, AVG};

#define VALIDATE_STRING_TYPE(k) \
  if (k->type != cJSON_String) \
    return "Invalid json: key is not a string"; \
  if (!strlen(k->valuestring)) \
    return "Invalid json: empty string is not allowed";

#define VALIDATE_KEY(j, key) \
  cJSON_GetObjectItem(j, #key); \
  if (!key) \
    return "Invalid json: missing " #key;

#define VALIDATE(j, key, Type) \
  VALIDATE_KEY(j, key) \
  if (key->type != Type) \
    return "Invalid json: " #key  "is not " #Type;

#define VALIDATE_STRING(j, key) \
  VALIDATE_KEY(j, key) \
  VALIDATE_STRING_TYPE(key)

#define VALIDATE_ARRAY(j, key) \
  VALIDATE(j, key, cJSON_Array) \
  sz = cJSON_GetArraySize(key); \
  if (!sz) \
    return "Invalid json: " #key " is empty";

#define VALIDATE_ENUM(js, key, validValues, msg) \
  VALIDATE(js, key, cJSON_String) \
  valid = 0; \
  esz = sizeof(validValues) / sizeof(validValues[0]); \
  for (j=0; !valid && j < esz; j++) \
    valid = strcasecmp( validValues[j], key->valuestring) == 0; \
  if (!valid) \
    return "Invalid json: " #key " is not one of: " msg;

/**
 * TS.CONF <name> <config json>
 * example TS.CONF user_report '{
 *   "key_fields": ["accountId", "deviceId"],
 *   "ts_fields": [
 *     { "field": "total_amount", "aggregation": "sum" },
 *     { "field": "page_views", "aggregation": "avg" }
 *   ],
 *   "interval": "hour",
 *   "keep_original": false
 *   }'
 *
 * */
char *ValidateTS(cJSON *conf, cJSON *data) {
  int valid, sz, i, esz, j;

  // verify keep_original parameter
  cJSON *keep_original = VALIDATE_KEY(conf, keep_original)
  if (keep_original->type != cJSON_True && keep_original->type != cJSON_False)
    return "Invalid json: keep_original is not a boolean";

  // verify interval parameter
  cJSON *interval = VALIDATE_ENUM(conf, interval, validIntervals, "second, minute, hour, day, week, month, year")

  // verify key_fields
  cJSON *key_fields = VALIDATE_ARRAY(conf, key_fields)
  for (i=0; i < sz; i++) {
    cJSON *k = cJSON_GetArrayItem(key_fields, i);
    VALIDATE_STRING_TYPE(k);
    if (data) {
      cJSON *tsfield = cJSON_GetObjectItem(data, k->valuestring);
      if (!tsfield) \
        return "Invalid data: missing field";
      VALIDATE_STRING_TYPE(tsfield);
    }
  }

  // verify time series fields
  cJSON *ts_fields = VALIDATE_ARRAY(conf, ts_fields)
  for (i=0; i < sz; i++) {
    cJSON *ts_field = cJSON_GetArrayItem(ts_fields, i);
    if (ts_field->type != cJSON_Object)
      return "Invalid json: ts_field is not an object";
    cJSON *field = VALIDATE_STRING(ts_field, field);
    cJSON *aggregation = VALIDATE_ENUM(ts_field, aggregation, validAggs, "sum, avg");
    if (data) {
      cJSON *tsfield = cJSON_GetObjectItem(data, field->valuestring);
      if (!tsfield)
        return "Invalid data: missing field";
      if (tsfield->type != cJSON_Number)
        return "Invalid data: agregation field is not a number";
    }
  }

  // All is good
  return NULL;
}


#define RMUTIL_ASSERT_NONULL(r, entry) RMUTIL_ASSERT_NOERROR(r) \
    else if (RedisModule_CallReplyType(r) == REDISMODULE_REPLY_NULL) { \
        char msg[1000] = "No such entry: "; \
        return RedisModule_ReplyWithError(ctx, strcat(msg, entry)); \
    }

long interval_timestamp(char *interval) {
  time_t t = time(NULL);
  struct tm st;
  gmtime_r(&t, &st);

  // TODO do we support week?
  st.tm_sec = 0; if (!strcasecmp(interval, SECOND)) return mktime(&st);
  st.tm_min = 0; if (!strcasecmp(interval, MINUTE)) return mktime(&st);
  st.tm_hour = 0; if (!strcasecmp(interval, HOUR)) return mktime(&st);
  st.tm_mday = 0; if (!strcasecmp(interval, DAY)) return mktime(&st);
  st.tm_mon = 0;
  return mktime(&st);
}

int TSAdd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // Time series entry name
  RedisModuleCallReply *confRep = RedisModule_Call(ctx, "HGET", "cs", "ts.conf", argv[1]);
  RMUTIL_ASSERT_NONULL(confRep, RedisModule_StringPtrLen(argv[1], NULL));

  // Time series entry conf previously stored for 'name'
  cJSON *conf;
  if (!(conf=cJSON_Parse(RedisModule_CallReplyStringPtr(confRep, NULL))))
    return RedisModule_ReplyWithError(ctx, "Something is wrong. Failed to parse ts conf");

  // Time series entry data
  cJSON *data;
  if (!(data=cJSON_Parse(RedisModule_StringPtrLen(argv[2], NULL))))
    return RedisModule_ReplyWithError(ctx, "Invalid json");

  const char *jsonErr;
  if ((jsonErr = ValidateTS(conf, data)))
    return RedisModule_ReplyWithError(ctx, jsonErr);

  // Create timestamp now (Use a single timestamp for all entries, not to accidently use different entries in case
  // during the calculation the time has changed)
  // TODO allow adding timestamp instead of 'now'
  long timestamp = interval_timestamp(cJSON_GetObjectItem(conf, "interval")->valuestring);

  char key_prefix[1000] = "ts.agg:";
  cJSON *key_fields = cJSON_GetObjectItem(conf, "key_fields");
  for (int i=0; i < cJSON_GetArraySize(key_fields); i++) {
    cJSON *k = cJSON_GetArrayItem(key_fields, i);
    cJSON *d = cJSON_GetObjectItem(data, k->valuestring);
    strcat(key_prefix, d->valuestring);
    strcat(key_prefix, ":");
  }

  cJSON *ts_fields = cJSON_GetObjectItem(conf, "ts_fields");
  for (int i=0; i < cJSON_GetArraySize(ts_fields); i++) {
    char agg_key[1000];

    cJSON *ts_field = cJSON_GetArrayItem(ts_fields, i);
    cJSON *field = cJSON_GetObjectItem(ts_field, "field");
    cJSON *aggregation = cJSON_GetObjectItem(ts_field, "aggregation");
    cJSON *datafield = cJSON_GetObjectItem(data, field->valuestring);
    sprintf(agg_key, "%s%s:%s", key_prefix, field->valuestring, aggregation->valuestring);

    // TODO can redis accept double?
    char value[100];
    char timestamp_key[100];
    char count_key[100];
    sprintf(value, "%lf", datafield->valuedouble);
    sprintf(timestamp_key, "%li", timestamp);
    sprintf(count_key, "%s:count", timestamp_key);
    //printf("=============\n%s\n%s\n%s\n", agg_key, timestamp_key, value);
    //RedisModuleCallReply *r = RedisModule_Call(ctx, "HSET", "ccl", agg_key, timestamp_key, (long long)(datafield->valueint));
    //RedisModuleCallReply *r = RedisModule_Call(ctx, "HSET", "clc", agg_key, timestamp, haRedisModule_CallReplyType(r)ckd);

    RedisModuleCallReply *r;
    if (!strcasecmp(aggregation->valuestring, "sum")) {
      r = RedisModule_Call(ctx, "HINCRBYFLOAT", "ccc", agg_key, timestamp_key, value);
      RMUTIL_ASSERT_NOERROR(r);
    } else if (!strcasecmp(aggregation->valuestring, "avg")) {
      long count = 0;
      double avg = 0, newval;
      char *eptr;

      // Current count
      r = RedisModule_Call(ctx, "HGET", "cc", agg_key, count_key);
      if (r && RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL) {
        count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);

        // Current average
        r = RedisModule_Call(ctx, "HGET", "cc", agg_key, timestamp_key);
        avg = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
      }

      // new average
      newval =(avg*count + datafield->valuedouble) / (count+1);
      sprintf(value, "%lf", newval);
      r = RedisModule_Call(ctx, "HSET", "ccc", agg_key, timestamp_key, value);
      RMUTIL_ASSERT_NOERROR(r);
    }

    r = RedisModule_Call(ctx, "HINCRBY", "ccl", agg_key, count_key, (long long)1);
    RMUTIL_ASSERT_NOERROR(r);

  }

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  //RedisModule_ReplyWithSimpleString(ctx, "All is good");
  return REDISMODULE_OK;
}

int TSConf(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {

  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  const char *name, *jsonErr, *conf;
  cJSON *json;

  name = RedisModule_StringPtrLen(argv[1], NULL);
  conf = RedisModule_StringPtrLen(argv[2], NULL);
  if (!(json=cJSON_Parse(conf)))
    return RedisModule_ReplyWithError(ctx, "Invalid json");

  if ((jsonErr = ValidateTS(json, NULL)))
    return RedisModule_ReplyWithError(ctx, jsonErr);

  RedisModuleCallReply *srep = RedisModule_Call(ctx, "HSET", "ccc", "ts.conf", name, conf);
  // Free the json before assert, otherwise invalid input will cause memory leak of the json
  cJSON_Delete(json);
  RMUTIL_ASSERT_NOERROR(srep);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  //RedisModule_ReplyWithCallReply(ctx, srep);
  return REDISMODULE_OK;
}

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

//#define RMUtil_AssertMSG(expr, msg) if (!(expr)) { fprintf (stderr, "Assertion '%s' Failed [%s]\n", __STRING(expr), msg); return REDISMODULE_ERR; }

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

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  // Register the timeseries module itself
  if (RedisModule_Init(ctx, "ts", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;

  // Register timeseries api
  RMUtil_RegisterWriteCmd(ctx, "ts.conf", TSConf);
  RMUtil_RegisterWriteCmd(ctx, "ts.add", TSAdd);

  // register the unit test
  RMUtil_RegisterWriteCmd(ctx, "ts.test", TestModule);

  return REDISMODULE_OK;
}
