#include "ts_entry.h"

TSObject *createTSObject(void) {
  struct TSObject *o;
  o = RedisModule_Calloc(1, sizeof(*o));
  return o;
}

void TSReleaseObject(struct TSObject *o) {
  RedisModule_Free(o->timefmt);
  RedisModule_Free(o->entry);
  RedisModule_Free(o);
}

void *TSRdbLoad(RedisModuleIO *rdb, int encver) {
  if (encver != 0) {
    /* RedisModule_Log("warning","Can't load data with version %d", encver);*/
    return NULL;
  }

  struct TSObject *o = createTSObject();

  o->init_timestamp = RedisModule_LoadUnsigned(rdb);
  o->interval = RedisModule_LoadUnsigned(rdb);

  size_t timefmt_length = RedisModule_LoadUnsigned(rdb);
  size_t len = 0;
  if (timefmt_length)
	  o->entry = (TSEntry *)RedisModule_LoadStringBuffer(rdb, &len);


  o->len = RedisModule_LoadUnsigned(rdb);
  if (o->len)
    o->entry = (TSEntry *)RedisModule_LoadStringBuffer(rdb, &len);

  return o;
}

void TSRdbSave(RedisModuleIO *rdb, void *value) {
  struct TSObject *o = value;

  RedisModule_SaveUnsigned(rdb, o->init_timestamp);
  RedisModule_SaveUnsigned(rdb, o->interval);

  size_t timefmt_length = strlen(o->timefmt);
  RedisModule_SaveUnsigned(rdb, timefmt_length);
  if (timefmt_length)
	  RedisModule_SaveStringBuffer(rdb,(const char *)o->timefmt, timefmt_length + 1);

  RedisModule_SaveUnsigned(rdb, o->len);
  if (o->len)
    RedisModule_SaveStringBuffer(rdb,(const char *)o->entry, o->len * sizeof(TSEntry));
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

RedisModuleType *createTSEntryType(RedisModuleCtx *ctx) {
  /* Name must be 9 chars... */
  return  RedisModule_CreateDataType(ctx, "timeserie", 0, TSRdbLoad, TSRdbSave, TSAofRewrite, TSDigest, TSFree);
}
