#include "ts_entry.h"

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

RedisModuleType *create_ts_entry_type(RedisModuleCtx *ctx) {
    /* Name must be 9 chars... */
    return RedisModule_CreateDataType(ctx, "timeserie", 0, TSRdbLoad, TSRdbSave, TSAofRewrite, TSDigest, TSFree);
}