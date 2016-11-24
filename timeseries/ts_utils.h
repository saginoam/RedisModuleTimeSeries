#ifndef _TS_UTILS_H_
#define _TS_UTILS_H_

#include "timeseries.h"
#include "ts_entry.h"

time_t interval2timestamp(Interval interval, const char *timestamp, const char *format);

time_t interval_timestamp(const char *interval, const char *timestamp, const char *format);

Interval str2interval(const char *interval);

const char *interval2str(Interval interval);

size_t idx_timestamp(time_t init_timestamp, size_t cur_timestamp, Interval interval);

char *doc_key_prefix(cJSON *conf, cJSON *data);

char *doc_agg_key(char *key_prefix, cJSON *ts_field);

double agg_value(cJSON *data, cJSON *ts_field);

#endif