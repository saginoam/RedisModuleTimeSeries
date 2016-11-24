#include "ts_entry.h"

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

char *doc_key_prefix(cJSON *conf, cJSON *data)
{
	static char key_prefix[1000] = "ts.doc:";
	cJSON *key_fields = cJSON_GetObjectItem(conf, "key_fields");
	for (int i=0; i < cJSON_GetArraySize(key_fields); i++) {
		cJSON *k = cJSON_GetArrayItem(key_fields, i);
		cJSON *d = cJSON_GetObjectItem(data, k->valuestring);
		strcat(key_prefix, d->valuestring);
		strcat(key_prefix, ":");
	}
	return key_prefix;
}

char *doc_agg_key(char *key_prefix, cJSON *ts_field){
	static char agg_key[1000];
	cJSON *field = cJSON_GetObjectItem(ts_field, "field");
	cJSON *aggregation = cJSON_GetObjectItem(ts_field, "aggregation");
	sprintf(agg_key, "%s%s:%s", key_prefix, field->valuestring, aggregation->valuestring);
	return agg_key;
}

double agg_value(cJSON *data, cJSON *ts_field) {
	cJSON *field = cJSON_GetObjectItem(ts_field, "field");
	return cJSON_GetObjectItem(data, field->valuestring)->valuedouble;
}

