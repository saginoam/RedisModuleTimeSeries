#ifndef __TEST_UTIL_H__
#define __TEST_UTIL_H__

#include "util.h"
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>


#define RMUtil_Test(f) \
                if (argc < 2 || RMUtil_ArgExists(__STRING(f), argv, argc, 1)) { \
                    int rc = f(ctx); \
                    if (rc != REDISMODULE_OK) { \
                        RedisModule_ReplyWithError(ctx, "Test " __STRING(f) " FAILED"); \
                        return REDISMODULE_ERR;\
                    }\
                }
           
                
#define RMUtil_Assert(expr) if (!(expr)) { fprintf (stderr, "Assertion '%s' Failed\n", __STRING(expr)); return REDISMODULE_ERR; }

#define RMUtil_AssertReplyEquals(rep, cstr) RMUtil_Assert( \
            RMUtil_StringEquals(RedisModule_CreateStringFromCallReply(rep), RedisModule_CreateString(ctx, cstr, strlen(cstr))) \
            )

#endif