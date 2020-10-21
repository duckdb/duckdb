//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

typedef unsigned char sb_symbol;

class FTSExtension : public Extension {
public:
	void Load(DuckDB &db) override;
};

} // namespace duckdb
