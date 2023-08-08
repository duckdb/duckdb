//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tpcds_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class TpcdsExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;

	//! Gets the specified TPC-DS Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-DS query
	static std::string GetAnswer(double sf, int query);
};

} // namespace duckdb
