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
	//! Generate the TPC-DS data of the given scale factor
	static void DSDGen(double flt_scale, duckdb::ClientContext &context, std::string schema = DEFAULT_SCHEMA,
	                   std::string suffix = "");

	//! Gets the specified TPC-DS Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-DS query
	static std::string GetAnswer(double sf, int query);
};

} // namespace tpcds
