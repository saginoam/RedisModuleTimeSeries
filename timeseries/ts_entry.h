#ifndef _TS_ENTRY_H_
#define _TS_ENTRY_H_

#include "timeseries.h"

typedef struct TSEntry {
  unsigned short count;
  double avg;
} TSEntry;

typedef struct TSObject {
  TSEntry *entry;
  size_t len;
  time_t init_timestamp;
  Interval interval;
  char *timefmt;
} TSObject;

RedisModuleType *createTSEntryType(RedisModuleCtx *ctx);

TSObject *createTSObject(void);

#endif
