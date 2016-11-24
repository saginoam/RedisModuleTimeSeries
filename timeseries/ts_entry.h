#ifndef _TS_ENTRY_
#define _TS_ENTRY_

#include "timeseries.h"

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

struct TSObject *createTSObject(void);

RedisModuleType *create_ts_entry_type(RedisModuleCtx *ctx);

#endif