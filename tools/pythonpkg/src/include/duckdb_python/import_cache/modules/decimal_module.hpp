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
	~DecimalCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		Decimal.LoadAttribute("Decimal", cache, *this);
	}

public:
	//! decimal.Decimal
	PythonImportCacheItem Decimal;
};

} // namespace duckdb
