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
#include "duckdb/main/extension_load_options.hpp"

namespace duckdb {
class ErrorData;

class ExtensionInfo {
public:
	ExtensionInfo();

	string orig_ext_name;
	string alias;
	mutex lock;
	atomic<bool> is_loaded;
	unique_ptr<ExtensionInstallInfo> install_info;
	unique_ptr<ExtensionLoadedInfo> load_info;
};

class ExtensionActiveLoad {
public:
	ExtensionActiveLoad(DatabaseInstance &db, ExtensionInfo &info, string extension_name_p, string alias_p,
	                    bool suffix_alias_p = false)
	    : db(db), load_lock(info.lock), info(info), extension_name(std::move(extension_name_p)),
	      alias(std::move(alias_p)), suffix_alias(suffix_alias_p) {};

	~ExtensionActiveLoad() = default;

	DatabaseInstance &db;
	unique_lock<mutex> load_lock;
	ExtensionInfo &info;
	string extension_name;
	string alias;
	bool suffix_alias = false;

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
	DUCKDB_API unique_ptr<ExtensionActiveLoad> BeginLoad(const ExtensionLoadOptions &options);

	DUCKDB_API void AddExternalExtensionAliasInternal(const string &alias, const string &extension_name);
	DUCKDB_API void AddExternalExtensionAlias(const string &alias, const string &extension_name);
	DUCKDB_API string GetExternalExtensionName(const string &alias);

	DUCKDB_API void SetExtensionLoadPrefix(const string &prefix);
	DUCKDB_API void SetExtensionLoadPrefixInternal(const string &prefix);
	DUCKDB_API void ClearExtensionLoadPrefix();
	DUCKDB_API string GetExtensionLoadPrefix();

	DUCKDB_API static ExtensionManager &Get(DatabaseInstance &db);
	DUCKDB_API static ExtensionManager &Get(ClientContext &context);

private:
	DatabaseInstance &db;
	mutex lock;
	unordered_map<string, unique_ptr<ExtensionInfo>> loaded_extensions_info;
	unordered_map<string, string> external_aliases;
	string extension_load_prefix;
};

} // namespace duckdb
