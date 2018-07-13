#ifndef PG_QUERY_JSON_PLPGSQL_H
#define PG_QUERY_JSON_PLPGSQL_H

#include "plpgsql.h"

char* plpgsqlToJSON(PLpgSQL_function* func);

#endif
