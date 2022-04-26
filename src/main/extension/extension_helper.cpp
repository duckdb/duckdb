#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"

#if defined(BUILD_ICU_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "icu-extension.hpp"
#endif

#if defined(BUILD_PARQUET_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "parquet-extension.hpp"
#endif

#if defined(BUILD_TPCH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "tpch-extension.hpp"
#endif

#if defined(BUILD_TPCDS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "tpcds-extension.hpp"
#endif

#if defined(BUILD_SUBSTRAIT_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "substrait-extension.hpp"
#endif

#if defined(BUILD_FTS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "fts-extension.hpp"
#endif

#if defined(BUILD_HTTPFS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "httpfs-extension.hpp"
#endif

#if defined(BUILD_VISUALIZER_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "visualizer-extension.hpp"
#endif

#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "json-extension.hpp"
#endif

#if defined(BUILD_EXCEL_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "excel-extension.hpp"
#endif

#if defined(BUILD_SQLSMITH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "sqlsmith-extension.hpp"
#endif

namespace duckdb {

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	unordered_set<string> extensions {"parquet",   "icu",        "tpch", "tpcds", "fts",     "httpfs",
	                                  "substrait", "visualizer", "json", "excel", "sqlsmith"};
	for (auto &ext : extensions) {
		LoadExtensionInternal(db, ext, true);
	}
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
	return LoadExtensionInternal(db, extension, false);
}

ExtensionLoadResult ExtensionHelper::LoadExtensionInternal(DuckDB &db, const std::string &extension,
                                                           bool initial_load) {
#ifdef DUCKDB_TEST_REMOTE_INSTALL
	if (!initial_load && StringUtil::Contains(DUCKDB_TEST_REMOTE_INSTALL, extension)) {
		Connection con(db);
		auto result = con.Query("INSTALL " + extension);
		if (!result->success) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		result = con.Query("LOAD " + extension);
		if (!result->success) {
			result->Print();
			return ExtensionLoadResult::EXTENSION_UNKNOWN;
		}
		return ExtensionLoadResult::LOADED_EXTENSION;
	}
#endif
	if (extension == "parquet") {
#if defined(BUILD_PARQUET_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#if defined(BUILD_ICU_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<ICUExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#if defined(BUILD_TPCH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<TPCHExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "substrait") {
#if defined(BUILD_SUBSTRAIT_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)

		db.LoadExtension<SubstraitExtension>();
#else
		// substrait extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#if defined(BUILD_TPCDS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<TPCDSExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#if defined(BUILD_FTS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<FTSExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#if defined(BUILD_HTTPFS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<HTTPFsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "visualizer") {
#if defined(BUILD_VISUALIZER_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<VisualizerExtension>();
#else
		// visualizer extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "json") {
#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<JSONExtension>();
#else
		// json extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "excel") {
#if defined(BUILD_EXCEL_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<EXCELExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "sqlsmith") {
#if defined(BUILD_SQLSMITH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
		db.LoadExtension<SQLSmithExtension>();
#else
		// excel extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else {
		// unknown extension
		return ExtensionLoadResult::EXTENSION_UNKNOWN;
	}
	return ExtensionLoadResult::LOADED_EXTENSION;
}

} // namespace duckdb
