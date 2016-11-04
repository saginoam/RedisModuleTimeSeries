#include "timeseries.h"

// TODO add expiration
// TODO add interval duration. i.e '10 minute'
// TODO Implement keep original
// TODO interval should be per field, not global
char *validIntervals[] = {SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR};

char *validAggs[] = {SUM, AVG};


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

time_t interval_timestamp(char *interval, const char *timestamp, const char *format) {
  struct tm st;
  if (timestamp) {
    // TODO Handle failure
    memset(&st, 0, sizeof(struct tm));
    strptime(timestamp, format, &st);
  }
  else {
    time_t t = time(NULL);
    gmtime_r(&t, &st);
  }

  // TODO do we support week?
  st.tm_sec =  0; if (!strcmp(interval, SECOND)) return mktime(&st);
  st.tm_min =  0; if (!strcmp(interval, MINUTE)) return mktime(&st);
  st.tm_hour = 0; if (!strcmp(interval, HOUR))   return mktime(&st);
  st.tm_mday = 0; if (!strcmp(interval, DAY))    return mktime(&st);
  st.tm_mon =  0;
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
  if (!(conf=cJSON_Parse(RedisModule_CallReplyStringPtr(confRep, NULL)))) {
    RedisModule_FreeCallReply(confRep);
    return RedisModule_ReplyWithError(ctx, "Something is wrong. Failed to parse ts conf");
  }

  // Time series entry data
  cJSON *data;
  if (!(data=cJSON_Parse(RedisModule_StringPtrLen(argv[2], NULL)))) {
    RedisModule_FreeCallReply(confRep);
    cJSON_Delete(data);
    return RedisModule_ReplyWithError(ctx, "Invalid json");
  }

  const char *jsonErr;
  if ((jsonErr = ValidateTS(conf, data))) {
    RedisModule_FreeCallReply(confRep);
    cJSON_Delete(data);
    return RedisModule_ReplyWithError(ctx, jsonErr);
  }

  // Create timestamp now (Use a single timestamp for all entries, not to accidently use different entries in case
  // during the calculation the time has changed)
  // TODO allow adding timestamp instead of 'now'
  long timestamp = interval_timestamp(cJSON_GetObjectItem(conf, "interval")->valuestring, NULL, NULL);

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
    sprintf(agg_key, "%s%s:%s", key_prefix, field->valuestring, aggregation->valuestring);
    cJSON *datafield = cJSON_GetObjectItem(data, field->valuestring);

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

    RedisModuleCallReply *r = NULL;
    if (!strcmp(aggregation->valuestring, SUM)) {
      r = RedisModule_Call(ctx, "HINCRBYFLOAT", "ccc", agg_key, timestamp_key, value);
      // TODO on error cJSON_Delete(data) and RedisModule_FreeCallReply(confRep);
      RMUTIL_ASSERT_NOERROR(r);
    } else if (!strcmp(aggregation->valuestring, AVG)) {
      long count = 0;
      double avg = 0, newval;
      char *eptr;

      // Current count
      r = RedisModule_Call(ctx, "HGET", "cc", agg_key, count_key);
      if (r && RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL) {
        count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);

        // Current average
        RedisModule_FreeCallReply(r);
        r = RedisModule_Call(ctx, "HGET", "cc", agg_key, timestamp_key);
        avg = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
      }

      // new average
      newval =(avg*count + datafield->valuedouble) / (count+1);
      sprintf(value, "%lf", newval);
      RedisModule_FreeCallReply(r);
      r = RedisModule_Call(ctx, "HSET", "ccc", agg_key, timestamp_key, value);
      // TODO on error cJSON_Delete(data);
      RMUTIL_ASSERT_NOERROR(r);
    }

    RedisModule_FreeCallReply(r);
    r = RedisModule_Call(ctx, "HINCRBY", "ccl", agg_key, count_key, (long long)1);
    // TODO on error cJSON_Delete(data) and RedisModule_FreeCallReply(r);;
    RMUTIL_ASSERT_NOERROR(r);
    RedisModule_FreeCallReply(r);

  }

  cJSON_Delete(data);
  cJSON_Delete(conf);
  RedisModule_FreeCallReply(confRep);
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

  if ((jsonErr = ValidateTS(json, NULL))) {
    cJSON_Delete(json);
    return RedisModule_ReplyWithError(ctx, jsonErr);
  }

  RedisModuleCallReply *srep = RedisModule_Call(ctx, "HSET", "ccc", "ts.conf", name, conf);
  // Free the json before assert, otherwise invalid input will cause memory leak of the json
  cJSON_Delete(json);
  RMUTIL_ASSERT_NOERROR(srep);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
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
