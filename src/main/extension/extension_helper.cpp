#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"

#if defined(BUILD_ICU_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define ICU_STATICALLY_LOADED true
#include "icu-extension.hpp"
#else
#define ICU_STATICALLY_LOADED false
#endif

#if defined(BUILD_PARQUET_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define PARQUET_STATICALLY_LOADED true
#include "parquet-extension.hpp"
#else
#define PARQUET_STATICALLY_LOADED false
#endif

#if defined(BUILD_TPCH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define TPCH_STATICALLY_LOADED true
#include "tpch-extension.hpp"
#else
#define TPCH_STATICALLY_LOADED false
#endif

#if defined(BUILD_TPCDS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define TPCDS_STATICALLY_LOADED true
#include "tpcds-extension.hpp"
#else
#define TPCDS_STATICALLY_LOADED false
#endif

#if defined(BUILD_SUBSTRAIT_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define SUBSTRAIT_STATICALLY_LOADED true
#include "substrait-extension.hpp"
#else
#define SUBSTRAIT_STATICALLY_LOADED false
#endif

#if defined(BUILD_FTS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define FTS_STATICALLY_LOADED true
#include "fts-extension.hpp"
#else
#define FTS_STATICALLY_LOADED false
#endif

#if defined(BUILD_HTTPFS_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define HTTPFS_STATICALLY_LOADED true
#include "httpfs-extension.hpp"
#else
#define HTTPFS_STATICALLY_LOADED false
#endif

#if defined(BUILD_VISUALIZER_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "visualizer-extension.hpp"
#endif

#if defined(BUILD_JSON_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#define JSON_STATICALLY_LOADED true
#include "json-extension.hpp"
#else
#define JSON_STATICALLY_LOADED false
#endif

#if defined(BUILD_EXCEL_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "excel-extension.hpp"
#endif

#if defined(BUILD_SQLSMITH_EXTENSION) && !defined(DISABLE_BUILTIN_EXTENSIONS)
#include "sqlsmith-extension.hpp"
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// Default Extensions
//===--------------------------------------------------------------------===//
static DefaultExtension internal_extensions[] = {
    {"icu", "Adds support for time zones and collations using the ICU library", ICU_STATICALLY_LOADED},
    {"parquet", "Adds support for reading and writing parquet files", PARQUET_STATICALLY_LOADED},
    {"tpch", "Adds TPC-H data generation and query support", TPCH_STATICALLY_LOADED},
    {"tpcds", "Adds TPC-DS data generation and query support", TPCDS_STATICALLY_LOADED},
    {"substrait", "Adds support for the Substrait integration", SUBSTRAIT_STATICALLY_LOADED},
    {"fts", "Adds support for Full-Text Search Indexes", FTS_STATICALLY_LOADED},
    {"httpfs", "Adds support for reading and writing files over a HTTP(S) connection", HTTPFS_STATICALLY_LOADED},
    {"json", "Adds support for JSON operations", JSON_STATICALLY_LOADED},
    {"sqlite_scanner", "Adds support for reading SQLite database files", false},
    {"postgres_scanner", "Adds support for reading from a Postgres database", false},
    {nullptr, nullptr, false}};

idx_t ExtensionHelper::DefaultExtensionCount() {
	idx_t index;
	for (index = 0; internal_extensions[index].name != nullptr; index++) {
	}
	return index;
}

DefaultExtension ExtensionHelper::GetDefaultExtension(idx_t index) {
	D_ASSERT(index < DefaultExtensionCount());
	return internal_extensions[index];
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	unordered_set<string> extensions {"parquet",   "icu",        "tpch", "tpcds", "fts",     "httpfs",
	                                  "substrait", "visualizer", "json", "excel", "sqlsmith"};
	for (auto &ext : extensions) {
		LoadExtensionInternal(db, ext, true);
	}
}

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
#if PARQUET_STATICALLY_LOADED
		db.LoadExtension<ParquetExtension>();
#else
		// parquet extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "icu") {
#if ICU_STATICALLY_LOADED
		db.LoadExtension<ICUExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpch") {
#if TPCH_STATICALLY_LOADED
		db.LoadExtension<TPCHExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "substrait") {
#if SUBSTRAIT_STATICALLY_LOADED

		db.LoadExtension<SubstraitExtension>();
#else
		// substrait extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "tpcds") {
#if TPCDS_STATICALLY_LOADED
		db.LoadExtension<TPCDSExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#if FTS_STATICALLY_LOADED
		db.LoadExtension<FTSExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#if HTTPFS_STATICALLY_LOADED
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
#if JSON_STATICALLY_LOADED
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
