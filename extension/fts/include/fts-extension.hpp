//===----------------------------------------------------------------------===//
//                         DuckDB
//
// fts-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "duckdb.hpp"

namespace duckdb {

class FTSExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
