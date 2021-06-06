#include "udf_struct_sqlite3.h"
#include "duckdb/function/scalar_function.hpp"

typedef void (*scalar_sqlite_udf_t)(sqlite3_context*, int, sqlite3_value**);

/* this flag is just used for checking if a UDF set an error message by calling sqlite3_result_error()*/
// #define SQLITE_ERROR 1

namespace duckdb {

struct SQLiteUDFWrapper {
public:
    static scalar_function_t CreateSQLiteScalarFunction(scalar_sqlite_udf_t sqlite_udf, void *pApp);
};

}