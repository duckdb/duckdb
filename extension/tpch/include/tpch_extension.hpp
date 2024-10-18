//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tpch_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TpchExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;

	//! Gets the specified TPC-H Query number as a string
	static std::string GetQuery(int query);
	//! Returns the CSV answer of a TPC-H query
	static std::string GetAnswer(double sf, int query);
};

} // namespace duckdb
