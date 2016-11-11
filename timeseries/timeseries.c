#include "timeseries.h"

// TODO README:
//   Examples
//   Elasticsearch compare
// TODO Features:
//   Expiration
//   interval duration. i.e '10 minute'
//   keep original
//   interval should be per field, not global
// TODO Redis questions:
//   memory leak
//   editing redis module sdk
//   using int/float as has key in api
//   redis-cli support for multi line
//   redis-cli support unescaped strings. i.e '{ "field": "val" }' instead of '{ \"field\": \"val\" }'
// TODO Testing:
//   Verify all keys exist
//   Verify all fields exist
//   Verify keep original

char *validIntervals[] = {SECOND, MINUTE, HOUR, DAY, MONTH, YEAR};

char *validAggs[] = {SUM, AVG};

void FreeCallReply(RedisModuleCallReply **rp) {
  if (*rp) {
    RedisModule_FreeCallReply(*rp);
    *rp = NULL;
  }
}

/**
 * TS.CONF <name> <config json>
 * example TS.CONF user_report '{
 *   "key_fields": ["accountId", "deviceId"],
 *   "ts_fields": [
 *     { "field": "total_amount", "aggregation": "sum" },
 *     { "field": "page_views", "aggregation": "avg" }
 *   ],
 *   "interval": "hour",
 *   "keep_original": false,
 *   "timeformat": "%Y:%m:%d %H:%M:%S"
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
  cJSON *interval = VALIDATE_ENUM(conf, interval, validIntervals, "second, minute, hour, day, month, year")

  long timestamp = interval_timestamp(interval->valuestring, cJSON_GetObjectString(data, "timestamp"),
      cJSON_GetObjectString(conf, "timeformat"));
  if (!timestamp)
    return "Invalid json: timestamp format and data mismatch";

  // verify key_fields
  cJSON *key_fields = VALIDATE_ARRAY(conf, key_fields)
  for (i=0; i < sz; i++) {
    cJSON *k = cJSON_GetArrayItem(key_fields, i);
    VALIDATE_STRING_TYPE(k);
    if (data) {
      cJSON *tsfield = cJSON_GetObjectItem(data, k->valuestring);
      if (!tsfield)
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
  memset(&st, 0, sizeof(struct tm));
  if (timestamp && format) {
    if (!strptime(timestamp, format, &st))
      return 0;
  }
  else {
    time_t t = time(NULL);
    gmtime_r(&t, &st);
  }

  if (!strcmp(interval, SECOND)) return mktime(&st);
  st.tm_sec =  0; if (!strcmp(interval, MINUTE)) return mktime(&st);
  st.tm_min =  0; if (!strcmp(interval, HOUR)) return mktime(&st);
  st.tm_hour = 0; if (!strcmp(interval, DAY))   return mktime(&st);
  st.tm_mday = 0; if (!strcmp(interval, MONTH))    return mktime(&st);
  st.tm_mon =  0;
  return mktime(&st);
}

int TSAdd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  cJSON *conf = NULL;
  cJSON *data = NULL;
  RedisModuleCallReply *confRep = NULL;

  void cleanup(void) {
    cJSON_Delete(data);
    cJSON_Delete(conf);
    RedisModule_FreeCallReply(confRep);
  }

  if (argc != 3) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_AutoMemory(ctx);

  // Time series entry name
  confRep = RedisModule_Call(ctx, "HGET", "cs", "ts.conf", argv[1]);
  RMUTIL_ASSERT_NONULL(confRep, RedisModule_StringPtrLen(argv[1], NULL), cleanup);

  // Time series entry conf previously stored for 'name'

  if (!(conf=cJSON_Parse(RedisModule_CallReplyStringPtr(confRep, NULL)))) {
    cleanup();
    return RedisModule_ReplyWithError(ctx, "Something is wrong. Failed to parse ts conf");
  }

  // Time series entry data
  if (!(data=cJSON_Parse(RedisModule_StringPtrLen(argv[2], NULL)))) {
    cleanup();
    return RedisModule_ReplyWithError(ctx, "Invalid json");
  }

  const char *jsonErr;
  if ((jsonErr = ValidateTS(conf, data))) {
    cleanup();
    return RedisModule_ReplyWithError(ctx, jsonErr);
  }

  // Create timestamp. Use a single timestamp for all entries, not to accidently use different entries in case
  // during the calculation the time has changed)
  long timestamp = interval_timestamp(cJSON_GetObjectItem(conf, "interval")->valuestring,
    cJSON_GetObjectString(data, "timestamp"), cJSON_GetObjectString(conf, "timeformat"));

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

    char value[100];
    char timestamp_key[100];
    char count_key[100];
    sprintf(value, "%lf", datafield->valuedouble);
    sprintf(timestamp_key, "%li", timestamp);
    sprintf(count_key, "%s:count", timestamp_key);

    RedisModuleCallReply *r = NULL;
    if (!strcmp(aggregation->valuestring, SUM)) {
      r = RedisModule_Call(ctx, "HINCRBYFLOAT", "ccc", agg_key, timestamp_key, value);
      RMUTIL_ASSERT_NOERROR(r, cleanup);
    } else if (!strcmp(aggregation->valuestring, AVG)) {
      long count = 0;
      double avg = 0, newval;
      char *eptr;

      // Current count
      r = RedisModule_Call(ctx, "HGET", "cc", agg_key, count_key);
      if (r && RedisModule_CallReplyType(r) != REDISMODULE_REPLY_NULL) {
        count = strtol(RedisModule_CallReplyStringPtr(r, NULL), &eptr, 10);

        // Current average
        RMCALL(r, RedisModule_Call(ctx, "HGET", "cc", agg_key, timestamp_key));
        avg = strtod(RedisModule_CallReplyStringPtr(r, NULL), &eptr);
      }

      // new average
      newval =(avg*count + datafield->valuedouble) / (count+1);
      sprintf(value, "%lf", newval);
      RMCALL(r, RedisModule_Call(ctx, "HSET", "ccc", agg_key, timestamp_key, value));
      RMUTIL_ASSERT_NOERROR(r, cleanup);
    }

    RMCALL(r, RedisModule_Call(ctx, "HINCRBY", "ccl", agg_key, count_key, (long long)1));
    RMUTIL_ASSERT_NOERROR(r, cleanup);
    FreeCallReply(&r);
  }

  cleanup();
  RedisModule_ReplyWithSimpleString(ctx, "OK");
  //RedisModule_ReplyWithSimpleString(ctx, "All is good");
  return REDISMODULE_OK;
}

int TSConf(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  void cleanup () {}
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
  RMUTIL_ASSERT_NOERROR(srep, cleanup);

  RedisModule_ReplyWithSimpleString(ctx, "OK");
  return REDISMODULE_OK;
}


/* ========================== "timeseries" type methods ======================= */

static RedisModuleType *TSType; //For now its just an array

struct TSObject {
  long *arr;
  size_t len;
};

struct TSObject *createTSObject(void) {
  struct TSObject *o;
  o = RedisModule_Alloc(sizeof(*o));
  o->arr = NULL;
  o->len = 0;
  return o;
}

void TSReleaseObject(struct TSObject *o) {
  RedisModule_Free(o->arr);
  RedisModule_Free(o);
}

void *TSRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver != 0) {
    /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
    return NULL;
  }

  struct TSObject *tso = createTSObject();
  tso->len = RedisModule_LoadUnsigned(rdb);
  size_t len = 0;
  if (tso->len)
    tso->arr = (long*)RedisModule_LoadStringBuffer(rdb, &len);

  return tso;
}

void TSRdbSave(RedisModuleIO *rdb, void *value) {
  struct TSObject *tso = value;
  RedisModule_SaveUnsigned(rdb,tso->len);
  if (tso->len)
    RedisModule_SaveStringBuffer(rdb,(const char *)tso->arr,tso->len * sizeof(long));
}

void TSAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
  struct TSObject *tso = value;
  if(tso->arr) {
    RedisModule_EmitAOF(aof,"TS.INSERT","sc",key,tso->arr);
  }
}

void TSDigest(RedisModuleDigest *digest, void *value) {
  /* TODO: The DIGEST module interface is yet not implemented. */
}

void TSFree(void *value) {
  printf("%d\n", __LINE__);
  TSReleaseObject(value);
}

/* Add new item to array
 * Currently increase in 1 item at a time.
 * TODO increase array size in chunks
 * */
void TSAddItem(struct TSObject *o, double value) {
  printf("Add item [%zu]\n", o->len);
  size_t newSize = o->len + 1;
  long *arr = RedisModule_Realloc(o->arr, sizeof(long) * newSize);
  // When implementing chunks needs to zero new bytes
  arr[o->len] = value;
  o->len = newSize;
  o->arr = arr;
}

int TSInsert(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 4)
    return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

  int type = RedisModule_KeyType(key);
  if (type != REDISMODULE_KEYTYPE_EMPTY && RedisModule_ModuleTypeGetType(key) != TSType)
    return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

  long long idx;
  if ((RedisModule_StringToLongLong(argv[2],&idx) != REDISMODULE_OK))
    return RedisModule_ReplyWithError(ctx,"ERR invalid index: must be a signed 64 bit integer");

  double value;
  if ((RedisModule_StringToDouble(argv[3],&value) != REDISMODULE_OK))
    return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a double");

  /* Create an empty value object if the key is currently empty. */
  struct TSObject *tso;
  if (type == REDISMODULE_KEYTYPE_EMPTY) {
    tso = createTSObject();
    RedisModule_ModuleTypeSetValue(key,TSType,tso);
  } else {
    tso = RedisModule_ModuleTypeGetValue(key);
  }

  /* Add new item */
  TSAddItem(tso, value);

  RedisModule_ReplyWithLongLong(ctx,tso->len);

  /* Didn't understand it yet. Just copied from example */
  //RedisModule_ReplicateVerbatim(ctx);

  return REDISMODULE_OK;
}

int TSGetIdx(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);

  if (argc != 3)
    return RedisModule_WrongArity(ctx);

  RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

  int type = RedisModule_KeyType(key);
  if (type == REDISMODULE_KEYTYPE_EMPTY)
    return RedisModule_ReplyWithError(ctx,"Key doesn't exist");

  if (RedisModule_ModuleTypeGetType(key) != TSType)
    return RedisModule_ReplyWithError(ctx,"Invalid keyy type");

  long long idx;
  if ((RedisModule_StringToLongLong(argv[2],&idx) != REDISMODULE_OK))
    return RedisModule_ReplyWithError(ctx,"ERR invalid index: must be a signed 64 bit integer");

  /* Create an empty value object if the key is currently empty. */
  struct TSObject *tso = RedisModule_ModuleTypeGetValue(key);

  if (tso->len <= idx) {
    return RedisModule_ReplyWithError(ctx,"ERR invalid index: index not exist");
  }

  RedisModule_ReplyWithLongLong(ctx,tso->arr[idx]);

  return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  // Register the timeseries module itself
  if (RedisModule_Init(ctx, "ts", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    return REDISMODULE_ERR;


  /* Name must be 9 chars... */
  TSType = RedisModule_CreateDataType(ctx,"timeserie",0,TSRdbLoad,TSRdbSave,TSAofRewrite,TSDigest,TSFree);
  if (TSType == NULL) return REDISMODULE_ERR;

  // Register timeseries api
  RMUtil_RegisterWriteCmd(ctx, "ts.conf", TSConf);
  RMUtil_RegisterWriteCmd(ctx, "ts.add", TSAdd);

  RMUtil_RegisterWriteCmd(ctx, "ts.insert", TSInsert);
  RMUtil_RegisterWriteCmd(ctx, "ts.getidx", TSGetIdx);

  // register the unit test
  RMUtil_RegisterWriteCmd(ctx, "ts.test", TestModule);

  return REDISMODULE_OK;
}
