//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ICUExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};


} // namespace duckdb
