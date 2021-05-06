#define DUCKDB_BUILD_LOADABLE_EXTENSION
// TODO lazy loading does not work on mingw compiler.
#ifdef __MINGW32__
#define DUCKDB_API __declspec(dllexport)
#define DUCKDB_CLASS_API
#endif
#include "duckdb.hpp"

using namespace duckdb;

inline string_t hello_fun(string_t what) {
	return "Hello, " + what.GetString();
}

extern "C" {
DUCKDB_EXTENSION_API void loadable_extension_demo_init(duckdb::DatabaseInstance &db) {
	Connection con(db);
	con.BeginTransaction();
	con.CreateScalarFunction<string_t, string_t>("hello", {LogicalType(LogicalTypeId::VARCHAR)},
	                                             LogicalType(LogicalTypeId::VARCHAR), &hello_fun);
	con.Commit();
}

DUCKDB_EXTENSION_API const char *loadable_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}
