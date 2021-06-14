#include "udf_struct_sqlite3.h"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

typedef void (*scalar_sqlite_udf_t)(sqlite3_context *, int, sqlite3_value **);

struct SQLiteUDFWrapper {
public:
	static duckdb::scalar_function_t CreateSQLiteScalarFunction(duckdb::scalar_sqlite_udf_t sqlite_udf, sqlite3 *db,
	                                                            void *pApp);
};

} // namespace duckdb
