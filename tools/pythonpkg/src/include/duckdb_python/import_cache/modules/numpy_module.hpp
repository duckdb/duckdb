
//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/numpy_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

//! Note: This class is generated using scripts.
//! If you need to add a new object to the cache you must:
//! 1. adjust tools/pythonpkg/scripts/imports.py
//! 2. run python3 tools/pythonpkg/scripts/generate_import_cache_json.py
//! 3. run python3 tools/pythonpkg/scripts/generate_import_cache_cpp.py
//! 4. run make format-main (the generator doesn't respect the formatting rules ;))

namespace duckdb {

struct NumpyMaCacheItem : public PythonImportCacheItem {

public:
	NumpyMaCacheItem(optional_ptr<PythonImportCacheItem> parent)
	    : PythonImportCacheItem("ma", parent), masked("masked", this), masked_array("masked_array", this) {
	}
	~NumpyMaCacheItem() override {
	}

	PythonImportCacheItem masked;
	PythonImportCacheItem masked_array;
};

struct NumpyCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "numpy";

public:
	NumpyCacheItem()
	    : PythonImportCacheItem("numpy"), ma(this), ndarray("ndarray", this), datetime64("datetime64", this),
	      generic("generic", this), int64("int64", this), bool_("bool_", this), byte("byte", this),
	      ubyte("ubyte", this), short_("short", this), ushort_("ushort", this), intc("intc", this),
	      uintc("uintc", this), int_("int_", this), uint("uint", this), longlong("longlong", this),
	      ulonglong("ulonglong", this), half("half", this), float16("float16", this), single("single", this),
	      longdouble("longdouble", this), csingle("csingle", this), cdouble("cdouble", this),
	      clongdouble("clongdouble", this) {
	}
	~NumpyCacheItem() override {
	}

	NumpyMaCacheItem ma;
	PythonImportCacheItem ndarray;
	PythonImportCacheItem datetime64;
	PythonImportCacheItem generic;
	PythonImportCacheItem int64;
	PythonImportCacheItem bool_;
	PythonImportCacheItem byte;
	PythonImportCacheItem ubyte;
	PythonImportCacheItem short_;
	PythonImportCacheItem ushort_;
	PythonImportCacheItem intc;
	PythonImportCacheItem uintc;
	PythonImportCacheItem int_;
	PythonImportCacheItem uint;
	PythonImportCacheItem longlong;
	PythonImportCacheItem ulonglong;
	PythonImportCacheItem half;
	PythonImportCacheItem float16;
	PythonImportCacheItem single;
	PythonImportCacheItem longdouble;
	PythonImportCacheItem csingle;
	PythonImportCacheItem cdouble;
	PythonImportCacheItem clongdouble;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace duckdb
