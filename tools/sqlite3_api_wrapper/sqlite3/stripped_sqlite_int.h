#ifndef __STRIPPED_SQLITE_INT__
#define __STRIPPED_SQLITE_INT__

#define LONGDOUBLE_TYPE long double
#include <stdint.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

typedef uint8_t u8;
typedef uint32_t u32;
typedef int64_t i64;
typedef uint64_t u64;

typedef int64_t sqlite3_int64;
typedef uint64_t sqlite_uint64;
typedef uint64_t sqlite3_uint64;

#ifdef USE_DUCKDB_SHELL_WRAPPER
#include "duckdb_shell_wrapper.h"
void *sqlite3_realloc64(void *ptr, sqlite3_uint64 n);
void *sqlite3_free(void *ptr);
#else
#define sqlite3_realloc64 realloc
#define sqlite3_free      free
#endif

#define sqlite3Malloc malloc
#define sqlite3IsNaN  isnan

#define ArraySize(X)   ((int)(sizeof(X) / sizeof(X[0])))
#define LARGEST_INT64  (0xffffffff | (((i64)0x7fffffff) << 32))
#define SMALLEST_INT64 (((i64)-1) - LARGEST_INT64)

#include <assert.h>

#define SQLITE_SKIP_UTF8(zIn)                                                                                          \
	{                                                                                                                  \
		if ((*(zIn++)) >= 0xc0) {                                                                                      \
			while ((*zIn & 0xc0) == 0x80) {                                                                            \
				zIn++;                                                                                                 \
			}                                                                                                          \
		}                                                                                                              \
	}

#ifndef MAX
#define MAX(A, B) ((A) > (B) ? (A) : (B))
#endif
#ifndef MIN
#define MIN(A, B) ((A) < (B) ? (A) : (B))
#endif

#ifndef SQLITE_MAX_LENGTH
#define SQLITE_MAX_LENGTH 1000000000
#endif

#endif
