#include "timeseries.h"

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
// TODO Coding
//   Reorder functions


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

time_t interval2timestamp(Interval interval, const char *timestamp, const char *format) {
	return interval_timestamp(interval2str(interval), timestamp, format);
}

time_t interval_timestamp(const char *interval, const char *timestamp, const char *format) {
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

Interval str2interval(const char *interval) {
	if (!strcmp(SECOND, interval)) return second;
	if (!strcmp(MINUTE, interval)) return minute;
	if (!strcmp(HOUR, interval)) return hour;
	if (!strcmp(DAY, interval)) return day;
	if (!strcmp(MONTH, interval)) return month;
	if (!strcmp(YEAR, interval)) return year;

	return none;
}

const char *interval2str(Interval interval) {
	if (interval == second) return SECOND;
	if (interval == minute) return MINUTE;
	if (interval == hour) return HOUR;
	if (interval == day) return DAY;
	if (interval == month) return MONTH;
	if (interval == year) return YEAR;

	return none;
}

size_t idx_timestamp(time_t init_timestamp, size_t cur_timestamp, Interval interval) {
	return difftime(cur_timestamp, init_timestamp) / interval;
}

/* ========================== "timeseries" type methods ======================= */

static RedisModuleType *TSType; //For now its just an array

typedef struct TSEntry {
	unsigned short count;
	double avg;
}TSEntry;

typedef struct TSObject {
	TSEntry *entry;
	size_t len;
	time_t init_timestamp;
	Interval interval;
	const char *timefmt;
}TSObject;

struct TSObject *createTSObject(void) {
	struct TSObject *o;
	o = RedisModule_Alloc(sizeof(*o));
	o->entry = NULL;
	o->len = 0;
	return o;
}

void TSReleaseObject(struct TSObject *o) {
	RedisModule_Free(o->entry);
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
		tso->entry = (TSEntry *)RedisModule_LoadStringBuffer(rdb, &len);

	return tso;
}

void TSRdbSave(RedisModuleIO *rdb, void *value) {
	struct TSObject *tso = value;
	RedisModule_SaveUnsigned(rdb,tso->len);
	if (tso->len)
		RedisModule_SaveStringBuffer(rdb,(const char *)tso->entry,tso->len * sizeof(double));
}

void TSAofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value) {
	struct TSObject *tso = value;
	if(tso->entry) {
		RedisModule_EmitAOF(aof,"TS.INSERT","sc",key,tso->entry);
	}
}

void TSDigest(RedisModuleDigest *digest, void *value) {
	/* TODO: The DIGEST module interface is yet not implemented. */
}

void TSFree(void *value) {
	TSReleaseObject(value);
}

/* =========================== time series api =====================================*/
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

	RedisModuleCallReply *srep = RedisModule_Call(ctx, "HSET", "ccc", "ts.conf", name, conf);
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

	RedisModule_ReplyWithSimpleString(ctx, "OK");

	return REDISMODULE_OK;
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

	char key_prefix[1000] = "ts.addagg:";
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
		ts_insert(ctx, strkey, datafield->valuedouble, cJSON_GetObjectString(data, "timestamp"));
	}

	cleanup();
	return RedisModule_ReplyWithSimpleString(ctx, "OK");
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
	//time_t timestamp = interval2timestamp(tso->interval, argc == 4 ? (char*)RedisModule_StringPtrLen(argv[3], NULL) : NULL, tso->timefmt);

	size_t from = idx_timestamp(tso->init_timestamp,
			interval2timestamp(tso->interval, timestamp, tso->timefmt), tso->interval);

	size_t to = (argc < 5) ? from : idx_timestamp(tso->init_timestamp,
			interval2timestamp(tso->interval, (char*)RedisModule_StringPtrLen(argv[4], NULL), tso->timefmt), tso->interval);

	if (tso->len <= to)
		to = tso->len - 1;

	if (!strcmp(op, "info")) {
		return RedisModule_ReplyWithString(ctx, RedisModule_CreateStringPrintf(ctx, "Start: %zu entries: %zu",
				tso->init_timestamp, tso->len));
	}

	if (tso->len <= from)
		return RedisModule_ReplyWithError(ctx,"ERR invalid value: timestamp not exist");

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

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
	// Register the timeseries module itself
	if (RedisModule_Init(ctx, "ts", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
		return REDISMODULE_ERR;


	/* Name must be 9 chars... */
	TSType = RedisModule_CreateDataType(ctx,"timeserie",0,TSRdbLoad,TSRdbSave,TSAofRewrite,TSDigest,TSFree);
	if (TSType == NULL) return REDISMODULE_ERR;

	// Register timeseries api
	RMUtil_RegisterWriteCmd(ctx, "ts.create", TSCreate);
	RMUtil_RegisterWriteCmd(ctx, "ts.insert", TSInsert);
	RMUtil_RegisterWriteCmd(ctx, "ts.get", TSGet);

	// Register timeseries doc api
	RMUtil_RegisterWriteCmd(ctx, "ts.createdoc", TSCreateDoc);
	RMUtil_RegisterWriteCmd(ctx, "ts.insertdoc", TSInsertDoc);

	// register the unit test
	RMUtil_RegisterWriteCmd(ctx, "ts.test", TestModule);

	return REDISMODULE_OK;
}
