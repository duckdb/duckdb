#include "udf_struct_sqlite3.h"
#include "duckdb/function/scalar_function.hpp"

typedef void (*scalar_sqlite_udf_t)(sqlite3_context*, int, sqlite3_value**);

namespace duckdb {

struct SQLiteUDFWrapper {
public:
    static scalar_function_t CreateSQLiteScalarFunction(scalar_sqlite_udf_t sqlite_udf);

    static scalar_function_t CreateBinarySQLiteFunction(scalar_sqlite_udf_t sqlite_udf);

};

}