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

struct PathLibCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "pathlib";

public:
	PathLibCacheItem() : PythonImportCacheItem("pathlib"), Path("Path", this) {
	}
	~PathLibCacheItem() override {
	}

public:
	PythonImportCacheItem Path;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
