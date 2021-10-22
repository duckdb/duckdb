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
};

} // namespace duckdb

extern "C" {
// TODO use DUCKDB_EXTENSION_API here
void icu_init(duckdb::DatabaseInstance &db);

const char *icu_version();
}