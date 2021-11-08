//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#define DUCKDB_BUILD_LOADABLE_EXTENSION

#include "duckdb.hpp"

namespace duckdb {

class ICUExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

} // namespace duckdb
