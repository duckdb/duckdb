//===----------------------------------------------------------------------===//
//                         DuckDB
//
// extension_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#ifdef BUILD_ICU_EXTENSION
#include "icu-extension.hpp"
#endif

#ifdef BUILD_PARQUET_EXTENSION
#include "parquet-extension.hpp"
#endif

#ifdef BUILD_TPCH_EXTENSION
#include "tpch-extension.hpp"
#endif

namespace duckdb {
class DuckDB;

enum class ExtensionLoadResult : uint8_t { LOADED_EXTENSION = 0, EXTENSION_UNKNOWN = 1, NOT_LOADED = 2 };

class ExtensionHelper {
public:
	static void LoadAllExtensions(DuckDB &db) {
#ifdef BUILD_ICU_EXTENSION
		db.LoadExtension<ICUExtension>();
#endif
#ifdef BUILD_PARQUET_EXTENSION
		db.LoadExtension<ParquetExtension>();
#endif
#ifdef BUILD_TPCH_EXTENSION
		db.LoadExtension<TPCHExtension>();
#endif
	}

	static ExtensionLoadResult LoadExtension(DuckDB &db, std::string extension) {
		if (extension == "parquet") {
#ifdef BUILD_PARQUET_EXTENSION
			db.LoadExtension<ParquetExtension>();
#else
			// parquet extension required but not build: skip this test
			return ExtensionLoadResult::NOT_LOADED;
#endif
		} else if (extension == "icu") {
#ifdef BUILD_ICU_EXTENSION
			db.LoadExtension<ICUExtension>();
#else
			// icu extension required but not build: skip this test
			return ExtensionLoadResult::NOT_LOADED;
#endif
		} else if (extension == "tpch") {
#ifdef BUILD_TPCH_EXTENSION
			db.LoadExtension<TPCHExtension>();
#else
			// icu extension required but not build: skip this test
			return ExtensionLoadResult::NOT_LOADED;
#endif
		} else {
			// unknown extension
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
};

} // namespace duckdb
