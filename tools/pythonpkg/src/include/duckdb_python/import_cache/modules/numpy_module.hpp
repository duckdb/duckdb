//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/import_cache/modules/numpy_module.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/import_cache/python_import_cache_item.hpp"

namespace duckdb {

struct NumpyCacheItem : public PythonImportCacheItem {
public:
	static constexpr const char *Name = "numpy";

public:
	~NumpyCacheItem() override {
	}
	virtual void LoadSubtypes(PythonImportCache &cache) override {
		ndarray.LoadAttribute("ndarray", cache, *this);
		datetime64.LoadAttribute("datetime64", cache, *this);
		int64.LoadAttribute("int64", cache, *this);
		generic.LoadAttribute("generic", cache, *this);
		int64.LoadAttribute("int64", cache, *this);
		bool_.LoadAttribute("bool_", cache, *this);
		byte.LoadAttribute("byte", cache, *this);
		ubyte.LoadAttribute("ubyte", cache, *this);
		short_.LoadAttribute("short_", cache, *this);
		ushort_.LoadAttribute("ushort_", cache, *this);
		intc.LoadAttribute("intc", cache, *this);
		uintc.LoadAttribute("uintc", cache, *this);
		int_.LoadAttribute("int_", cache, *this);
		uint.LoadAttribute("uint", cache, *this);
		longlong.LoadAttribute("longlong", cache, *this);
		ulonglong.LoadAttribute("ulonglong", cache, *this);
		half.LoadAttribute("half", cache, *this);
		float16.LoadAttribute("float16", cache, *this);
		single.LoadAttribute("single", cache, *this);
		longdouble.LoadAttribute("longdouble", cache, *this);
		csingle.LoadAttribute("csingle", cache, *this);
		cdouble.LoadAttribute("cdouble", cache, *this);
		clongdouble.LoadAttribute("clongdouble", cache, *this);
	}

public:
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
};

} // namespace duckdb
