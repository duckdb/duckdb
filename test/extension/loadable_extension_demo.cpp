#include "duckdb.hpp"

using namespace duckdb;

inline string_t hello_fun(string_t what) {
	return "Hello, " + what.GetString();
}

extern "C" {
void loadable_extension_demo_init(duckdb::DatabaseInstance &db) {
	Connection con(db);
	con.BeginTransaction();
	con.CreateScalarFunction<string_t, string_t>("hello", {LogicalType::VARCHAR}, LogicalType::VARCHAR, &hello_fun);
	con.Commit();
}

const char *loadable_extension_demo_version() {
	return DuckDB::LibraryVersion();
}
}