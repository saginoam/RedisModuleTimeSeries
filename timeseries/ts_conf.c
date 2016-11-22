/* ========================== "tsconfigr" type methods ======================= */
// name, interval, key fields, agg fields, timefmt (keep original, metadata)

static RedisModuleType *TSConfig;

typedef struct TSConfigObject {
  const char *name;
  Interval interval;
  const char **key_fields;
  size_t key_fields_length;
  const char **agg_fields;
  size_t agg_fields_length;
  const char **metadata_fields;
  size_t metadata_fields_length;
  const char *timefmt;
} TSConfigObject;

struct TSConfigObject *createTSConfigObject(void) {
  struct TSConfigObject *o;
  o = RedisModule_Calloc(1, sizeof(*o));
  return o;
}

void TSConfigReleaseObject(struct TSConfigObject *o) {
  RedisModule_Free(o->name);
  for (int i = 0; i < o->key_fields_length; i++) RedisModule_Free(o->key_fields[i]);
  for (int i = 0; i < o->agg_fields_length; i++) RedisModule_Free(o->agg_fields[i]);
  for (int i = 0; i < o->metadata_fields_length; i++) RedisModule_Free(o->metadata_fields[i]);
  RedisModule_Free(o->timefmt);
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


