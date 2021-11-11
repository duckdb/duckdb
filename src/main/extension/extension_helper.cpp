#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"

#ifdef BUILD_ICU_EXTENSION
#include "icu-extension.hpp"
#endif

#ifdef BUILD_PARQUET_EXTENSION
#include "parquet-extension.hpp"
#endif

#ifdef BUILD_TPCH_EXTENSION
#include "tpch-extension.hpp"
#endif

#ifdef BUILD_TPCDS_EXTENSION
#include "tpcds-extension.hpp"
#endif

#ifdef BUILD_FTS_EXTENSION
#include "fts-extension.hpp"
#endif

#ifdef BUILD_HTTPFS_EXTENSION
#include "httpfs-extension.hpp"
#endif

#ifdef BUILD_VISUALIZER_EXTENSION
#include "visualizer-extension.hpp"
#endif

namespace duckdb {

void ExtensionHelper::LoadAllExtensions(DuckDB &db) {
	unordered_set<string> extensions {"parquet", "icu", "tpch", "tpcds", "fts", "httpfs", "visualizer"};
	for (auto &ext : extensions) {
		LoadExtension(db, ext);
	}
}

//===--------------------------------------------------------------------===//
// Load Statically Compiled Extension
//===--------------------------------------------------------------------===//
ExtensionLoadResult ExtensionHelper::LoadExtension(DuckDB &db, const std::string &extension) {
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
	} else if (extension == "tpcds") {
#ifdef BUILD_TPCDS_EXTENSION
		db.LoadExtension<TPCDSExtension>();
#else
		// icu extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "fts") {
#ifdef BUILD_FTS_EXTENSION
		db.LoadExtension<FTSExtension>();
#else
		// fts extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "httpfs") {
#ifdef BUILD_HTTPFS_EXTENSION
		db.LoadExtension<HTTPFsExtension>();
#else
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else if (extension == "visualizer") {
#ifdef BUILD_VISUALIZER_EXTENSION
		db.LoadExtension<VisualizerExtension>();
#else
		// visualizer extension required but not build: skip this test
		return ExtensionLoadResult::NOT_LOADED;
#endif
	} else {
		// unknown extension
		return ExtensionLoadResult::EXTENSION_UNKNOWN;
	}
	return ExtensionLoadResult::LOADED_EXTENSION;
}

} // namespace duckdb
