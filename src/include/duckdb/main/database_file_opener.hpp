//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/common/http_util.hpp"

namespace duckdb {
class DatabaseInstance;

class DatabaseFileOpener : public FileOpener {
public:
	explicit DatabaseFileOpener(DatabaseInstance &db_p) : db(db_p) {
	}

	Logger &GetLogger() const override {
		return Logger::Get(db);
	}

	SettingLookupResult TryGetCurrentSetting(const Identifier &key, Value &result) override {
		return db.TryGetCurrentSetting(key, result);
	}

	SettingLookupResult TryGetCurrentSetting(const Identifier &key, Value &result, FileOpenerInfo &) override {
		return db.TryGetCurrentSetting(key, result);
	}

	optional_ptr<ClientContext> TryGetClientContext() override {
		return nullptr;
	}

	optional_ptr<DatabaseInstance> TryGetDatabase() override {
		return &db;
	}
	HTTPUtil &GetHTTPUtil() override {
		return HTTPUtil::Get(*TryGetDatabase());
	}

private:
	DatabaseInstance &db;
};

class DatabaseFileSystem : public OpenerFileSystem {
public:
	explicit DatabaseFileSystem(DatabaseInstance &db_p) : db(db_p), database_opener(db_p) {
	}

	FileSystem &GetFileSystem() const override {
		auto &config = DBConfig::GetConfig(db);
		return *config.file_system;
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return &database_opener;
	}

private:
	DatabaseInstance &db;
	mutable DatabaseFileOpener database_opener;
};

class LocalDatabaseFileSystem : public OpenerFileSystem {
public:
	//! `apply_disabled_file_systems` may only be false where the paths reached are engine-internal and not
	//! user-controllable, since it is what makes `disabled_filesystems` bite on a local path.
	explicit LocalDatabaseFileSystem(DatabaseInstance &db_p, bool apply_disabled_file_systems = true);

	FileSystem &GetFileSystem() const override;
	optional_ptr<FileOpener> GetOpener() const override {
		return &database_opener;
	}

private:
	DatabaseInstance &db;
	unique_ptr<FileSystem> owned_file_system;
	FileSystem &local_fs;
	bool apply_disabled_file_systems;
	mutable DatabaseFileOpener database_opener;
};

} // namespace duckdb
