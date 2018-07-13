#ifndef PG_QUERY_FINGERPRINT_H
#define PG_QUERY_FINGERPRINT_H

#include <stdbool.h>

PgQueryFingerprintResult pg_query_fingerprint_with_opts(const char* input, bool printTokens);

#endif
