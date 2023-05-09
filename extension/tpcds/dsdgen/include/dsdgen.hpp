//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// dsdgen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#endif

namespace duckdb {
class ClientContext;
}

namespace tpcds {

struct DSDGenWrapper {
	//! Create the TPC-DS tables in the given schema with the given suffix
	static void CreateTPCDSSchema(duckdb::ClientContext &context, std::string catalog, std::string schema, std::string suffix, bool keys,
	                              bool overwrite);
	//! Generate the TPC-DS data of the given scale factor
	static void DSDGen(double scale, duckdb::ClientContext &context, std::string catalog, std::string schema, std::string suffix);

	static uint32_t QueriesCount();
	//! Gets the specified TPC-DS Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-DS query
	static std::string GetAnswer(double sf, int query);
};

} // namespace tpcds
