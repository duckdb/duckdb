
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/pathlib_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct PathlibCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pathlib";

public:
	PathlibCacheItem() : PythonImportCacheItem("pathlib"), Path("Path", this) {
	}
	~PathlibCacheItem() override {
	}

	PythonImportCacheItem Path;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
