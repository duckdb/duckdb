
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/decimal_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct DecimalCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "decimal";

public:
	DecimalCacheItem() : PythonImportCacheItem("decimal"), Decimal("Decimal", this) {
	}
	~DecimalCacheItem() override {
	}

	PythonImportCacheItem Decimal;
};

} // namespace duckdb
