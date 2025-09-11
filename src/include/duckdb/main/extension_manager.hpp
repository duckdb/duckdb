//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/main/extension_install_info.hpp"

namespace duckdb {
class ErrorData;

class ExtensionInfo {
public:
	ExtensionInfo();

	mutex lock;
	atomic<bool> is_loaded;
	unique_ptr<ExtensionInstallInfo> install_info;
	unique_ptr<ExtensionLoadedInfo> load_info;
};

class ExtensionActiveLoad {
public:
	ExtensionActiveLoad(DatabaseInstance &db, ExtensionInfo &info, string extension_name);

	DatabaseInstance &db;
	unique_lock<mutex> load_lock;
	ExtensionInfo &info;
	string extension_name;

public:
	void FinishLoad(ExtensionInstallInfo &install_info);
	void LoadFail(const ErrorData &error);
};

class ExtensionManager {
public:
	explicit ExtensionManager(DatabaseInstance &db);

	DUCKDB_API bool ExtensionIsLoaded(const string &name);
	DUCKDB_API vector<string> GetExtensions();
	DUCKDB_API optional_ptr<ExtensionInfo> GetExtensionInfo(const string &name);
	DUCKDB_API unique_ptr<ExtensionActiveLoad> BeginLoad(const string &extension);

	DUCKDB_API static ExtensionManager &Get(DatabaseInstance &db);
	DUCKDB_API static ExtensionManager &Get(ClientContext &context);

private:
	DatabaseInstance &db;
	mutex lock;
	unordered_map<string, unique_ptr<ExtensionInfo>> loaded_extensions_info;
};

} // namespace duckdb
