//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// dbgen.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb.hpp"

namespace duckdb {
class ClientContext;
}

namespace tpch {


struct DBGenWrapper {
	//! Create the TPC-H tables in the given schema with the given suffix
	static void CreateTPCHSchema(duckdb::ClientContext &context, string schema, string suffix);
	//! Load the TPC-H data of the given scale factor
	static void LoadTPCHData(duckdb::ClientContext &context, double sf, string schema, string suffix);

	//! Gets the specified TPC-H Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-H query
	static std::string GetAnswer(double sf, int query);
};

} // namespace tpch
