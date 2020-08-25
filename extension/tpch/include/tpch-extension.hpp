//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tpch-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TPCHExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};


} // namespace duckdb
