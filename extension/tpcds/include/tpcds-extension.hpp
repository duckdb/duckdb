//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tpcds-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class TPCDSExtension : public Extension {
public:
	void Load(DuckDB &db) override;

	//! Gets the specified TPC-DS Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-DS query
	static std::string GetAnswer(double sf, int query);
};

} // namespace duckdb

extern "C" {
// TODO use DUCKDB_EXTENSION_API here
void tpcds_init(duckdb::DatabaseInstance &db);

const char *tpcds_version();
}