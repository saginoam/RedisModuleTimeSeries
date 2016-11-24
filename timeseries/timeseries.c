#include "timeseries.h"
#include "ts_entry.h"
#include "ts_utils.h"

// TODO API:
//   Get Range
// TODO README:
//   Examples
//   Script for generating many entries
//   Elasticsearch compare
//   Missing features
// TODO Features:
//   Expiration
//   interval duration. i.e '10 minute'
//   keep original
//   interval should be per field, not global
//   pagination
// TODO Redis questions:
//   Persistency and load from disk
// TODO Testing:
//   All 3 new APIs
//   Verify all keys exist
//   Verify all fields exist
//   Verify keep original
// TODO Usability
//   Command line help for api
//   Last inserted timestamp/offset, so client can know from where to begin

static char *validIntervals[] = {SECOND, MINUTE, HOUR, DAY, MONTH, YEAR};

static char *validAggs[] = {SUM, AVG};

static RedisModuleType *TSType;

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
    cJSON *keep_original = VALIDATE_KEY(conf, keep_original);
    if (keep_original->type != cJSON_True && keep_original->type != cJSON_False)
        return "Invalid json: keep_original is not a boolean";

    // verify interval parameter
    cJSON *interval = VALIDATE_ENUM(conf, interval, validIntervals, "second, minute, hour, day, month, year");
    long timestamp = interval_timestamp(interval->valuestring, cJSON_GetObjectString(data, "timestamp"),
        cJSON_GetObjectString(conf, "timeformat"));
    if (!timestamp)
        return "Invalid json: timestamp format and data mismatch";

    // verify key_fields
    cJSON *key_fields = VALIDATE_ARRAY(conf, key_fields);
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
    cJSON *ts_fields = VALIDATE_ARRAY(conf, ts_fields);
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

int TSCreateDoc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

    RedisModuleCallReply *srep = RedisModule_Call(ctx, "HSET", "ccc", "ts.doc", name, conf);
    // Free the json before assert, otherwise invalid input will cause memory leak of the json
    cJSON_Delete(json);
    RMUTIL_ASSERT_NOERROR(srep, cleanup);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}

/* Add new item to array
 * Currently increase in 1 item at a time.
 * TODO increase array size in chunks
 * TODO Handle reached entries limit
 * */
void TSAddItem(struct TSObject *o, double value, time_t timestamp) {
    size_t idx = idx_timestamp(o->init_timestamp, timestamp, o->interval);

    if (idx >= o->len) {
        size_t newSize = idx + 1;
        o->entry = RedisModule_Realloc(o->entry, sizeof(TSEntry) * newSize);
        bzero(&o->entry[o->len], sizeof(TSEntry) * (newSize - o->len));
        o->len = newSize;
    }

    TSEntry *e = &o->entry[idx];
    e->avg = (e->avg * e->count + value) / (e->count + 1);
    e->count++;
}

int ts_insert(RedisModuleCtx *ctx, RedisModuleString *name, double value, char *timestamp_str) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, name, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
        return RedisModule_ReplyWithError(ctx, "key doesn't exist");
    if (RedisModule_ModuleTypeGetType(key) != TSType)
        return RedisModule_ReplyWithError(ctx, "key is not time series");
    struct TSObject *tso = RedisModule_ModuleTypeGetValue(key);

    time_t timestamp = interval2timestamp(tso->interval, timestamp_str, tso->timefmt);
    if (!timestamp)
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: Time Stamp is not valid");

    if (timestamp < tso->init_timestamp)
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: Time Stamp is too early");
    /* Add new item */
    TSAddItem(tso, value, timestamp);
    RedisModule_ReplyWithSimpleString(ctx, "OK");

    /* Didn't understand it yet. Just copied from example */
    //RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

int TSInsert(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 4)
        return RedisModule_WrongArity(ctx);

    double value;
    if ((RedisModule_StringToDouble(argv[2],&value) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx,"ERR invalid value: must be a double");

    return ts_insert(ctx, argv[1], value, argc == 4 ? (char*)RedisModule_StringPtrLen(argv[3], NULL) : NULL);
}

int ts_create(RedisModuleCtx *ctx, RedisModuleString *name, const char *interval, const char *timefmt, const char *timestamp) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, name, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY)
        return RedisModule_ReplyWithError(ctx,"key already exist");

    struct TSObject *tso = createTSObject();
    RedisModule_ModuleTypeSetValue(key, TSType, tso);
    tso->interval = str2interval(interval);
    if (tso->interval == none)
        return RedisModule_ReplyWithError(ctx,"Invalid interval. Must be one of: second, minute, hour, day, month, year");
    tso->timefmt = timefmt;
    tso->init_timestamp = interval_timestamp(interval, timestamp, tso->timefmt);

    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int TSCreate(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 4)
        return RedisModule_WrongArity(ctx);

    return ts_create(ctx, argv[1], RedisModule_StringPtrLen(argv[2], NULL),
        DEFAULT_TIMEFMT, argc == 4 ? RedisModule_StringPtrLen(argv[3], NULL) : NULL);
}

int TSInsertDoc(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    cJSON *conf = NULL;
    cJSON *data = NULL;
    RedisModuleCallReply *confRep = NULL;
    const char *jsonErr;

    void cleanup(void) {
        cJSON_Delete(data);
        cJSON_Delete(conf);
        RedisModule_FreeCallReply(confRep);
    }

    int exit_status(int status) {
        cleanup();
        return status;
    }

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    // Time series entry name
    confRep = RedisModule_Call(ctx, "HGET", "cs", "ts.doc", argv[1]);
    RMUTIL_ASSERT_NONULL(confRep, RedisModule_StringPtrLen(argv[1], NULL), cleanup);

    // Time series entry conf previously stored for 'name'
    if (!(conf=cJSON_Parse(RedisModule_CallReplyStringPtr(confRep, NULL))))
        return exit_status(RedisModule_ReplyWithError(ctx, "Something is wrong. Failed to parse ts conf"));

    // Time series entry data
    if (!(data=cJSON_Parse(RedisModule_StringPtrLen(argv[2], NULL))))
        return exit_status(RedisModule_ReplyWithError(ctx, "Invalid json"));

    if ((jsonErr = ValidateTS(conf, data)))
        return exit_status(RedisModule_ReplyWithError(ctx, jsonErr));

    // Create timestamp. Use a single timestamp for all entries, not to accidently use different entries in case
    // during the calculation the time has changed)
    long timestamp = interval_timestamp(cJSON_GetObjectItem(conf, "interval")->valuestring,
        cJSON_GetObjectString(data, "timestamp"), cJSON_GetObjectString(conf, "timeformat"));

    char *key_prefix = doc_key_prefix(conf, data);

    cJSON *ts_fields = cJSON_GetObjectItem(conf, "ts_fields");
    for (int i=0; i < cJSON_GetArraySize(ts_fields); i++) {
        char *agg_key = doc_agg_key(key_prefix, cJSON_GetArrayItem(ts_fields, i));
        double value = agg_value(data, cJSON_GetArrayItem(ts_fields, i));

        char timestamp_key[100];
        sprintf(timestamp_key, "%li", timestamp);

        RedisModuleString *strkey = RedisModule_CreateStringPrintf(ctx, "%s", agg_key);
        RedisModuleKey *key = RedisModule_OpenKey(ctx, strkey, REDISMODULE_READ | REDISMODULE_WRITE);
        if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
            ts_create(ctx, strkey, cJSON_GetObjectItem(conf, "interval")->valuestring, DEFAULT_TIMEFMT,
                    cJSON_GetObjectString(data, "timestamp"));
        } else if (RedisModule_ModuleTypeGetType(key) != TSType) {
            return RedisModule_ReplyWithError(ctx, "key is not time series");
        }
        RedisModule_CloseKey(key);

        //TODO check errors
        ts_insert(ctx, strkey, value, cJSON_GetObjectString(data, "timestamp"));
    }

    return exit_status(RedisModule_ReplyWithSimpleString(ctx, "OK"));
}

int TSGet(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3 || argc > 5)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
        return RedisModule_ReplyWithError(ctx,"Key doesn't exist");

    if (RedisModule_ModuleTypeGetType(key) != TSType)
        return RedisModule_ReplyWithError(ctx,"Invalid key type");

    char *op = (char*)RedisModule_StringPtrLen(argv[2], NULL);
    struct TSObject *tso = RedisModule_ModuleTypeGetValue(key);

    const char *timestamp = (argc > 3) ? (char*)RedisModule_StringPtrLen(argv[3], NULL) : NULL;

    size_t from = idx_timestamp(tso->init_timestamp,
    	interval2timestamp(tso->interval, timestamp, tso->timefmt), tso->interval);

    size_t to = (argc < 5) ? from : idx_timestamp(tso->init_timestamp,
        interval2timestamp(tso->interval, (char*)RedisModule_StringPtrLen(argv[4], NULL), tso->timefmt), tso->interval);

    if (tso->len <= to)
        to = tso->len - 1;

    if (tso->len <= from) {
        RedisModuleString *ret = RedisModule_CreateStringPrintf(ctx,
            "ERR invalid value: timestamp not exist len: %zu from: %zu to: %zu", tso->len, from, to);
        return RedisModule_ReplyWithError(ctx, RedisModule_StringPtrLen(ret, NULL));
    }

    if (to < from)
        return RedisModule_ReplyWithError(ctx,"ERR invalid range: end before start");

    RedisModule_ReplyWithArray(ctx,to - from + 1);
    for (size_t i = from; i <= to; i++) {
        TSEntry *e = &tso->entry[i];
        if (!strcmp(op, AVG))
            RedisModule_ReplyWithDouble(ctx, e->avg);
        else if (!strcmp(op, SUM))
            RedisModule_ReplyWithDouble(ctx, e->avg * e->count);
        else if (!strcmp(op, COUNT))
            RedisModule_ReplyWithLongLong(ctx, e->count);
        else
            return RedisModule_ReplyWithError(ctx,"ERR invalid operation: must be one of avg, sum, count");
    }

    return REDISMODULE_OK;
}

int TSInfo(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    char starttimestr[64], endtimestr[64];

    if (argc != 2)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY)
        return RedisModule_ReplyWithError(ctx,"Key doesn't exist");

    if (RedisModule_ModuleTypeGetType(key) != TSType)
        return RedisModule_ReplyWithError(ctx,"Invalid key type");

    struct TSObject *tso = RedisModule_ModuleTypeGetValue(key);

    size_t idx = tso->len - (tso->len ? 1 : 0); // Index is len - 1, unless no entries at all.
    time_t endtime = tso->init_timestamp + tso->interval * idx;
    struct tm st;

    localtime_r(&tso->init_timestamp, &st);
    strftime(starttimestr, 64, tso->timefmt, &st);
    localtime_r(&endtime, &st);
    strftime(endtimestr, 64, tso->timefmt, &st);

    RedisModuleString *ret = RedisModule_CreateStringPrintf(ctx, "Start: %s End: %s len: %zu Interval: %s",
        starttimestr, endtimestr, tso->len, interval2str(tso->interval));
    return RedisModule_ReplyWithString(ctx, ret);

}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    // Register the timeseries module itself
    if (RedisModule_Init(ctx, "ts", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    TSType = create_ts_entry_type(ctx);
    if (TSType == NULL) return REDISMODULE_ERR;

    // Register timeseries api
    RMUtil_RegisterWriteCmd(ctx, "ts.create", TSCreate);
    RMUtil_RegisterWriteCmd(ctx, "ts.insert", TSInsert);
    RMUtil_RegisterWriteCmd(ctx, "ts.get", TSGet);
    RMUtil_RegisterWriteCmd(ctx, "ts.info", TSInfo);

    // Register timeseries doc api
    RMUtil_RegisterWriteCmd(ctx, "ts.createdoc", TSCreateDoc);
    RMUtil_RegisterWriteCmd(ctx, "ts.insertdoc", TSInsertDoc);

    // register the unit test
    RMUtil_RegisterWriteCmd(ctx, "ts.test", TestModule);

    return REDISMODULE_OK;
}
