#include "duckdb.hpp"


#ifdef _WIN32
#define DEMO_API __declspec(dllexport)
#else
#define DEMO_API
#endif


using namespace duckdb;

inline string_t hello_fun(string_t what) {
	return "Hello, " + what.GetString();
}

extern "C" {
DEMO_API void loadable_extension_demo_init(duckdb::DatabaseInstance &db) {
	Connection con(db);
	con.BeginTransaction();
	con.CreateScalarFunction<string_t, string_t>("hello", {LogicalType(LogicalTypeId::VARCHAR) }, LogicalType(LogicalTypeId::VARCHAR) , &hello_fun);
	con.Commit();
}

DEMO_API const char *loadable_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}