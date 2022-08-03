#define DUCKDB_EXTENSION_MAIN

#include "include/sketch-extension.hpp"
#include "include/sketch-hll.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog.hpp"

#include <cassert>

namespace duckdb {

void SketchExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	SketchHll::RegisterFunction(*con.context);
	con.Commit();
}

std::string SketchExtension::Name() {
	return "sketch";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void sketch_init(duckdb::DatabaseInstance &db) { // NOLINT
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SketchExtension>();
}

DUCKDB_EXTENSION_API const char *sketch_version() { // NOLINT
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
