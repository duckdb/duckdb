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
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/http/http_util.hpp"

namespace duckdb {
class DatabaseInstance;

class DatabaseFileOpener : public FileOpener {
public:
	explicit DatabaseFileOpener(DatabaseInstance &db_p) : db(db_p), context(db_p.GetContext()) {
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

	Context &TryGetContext() override {
		return context;
	}

	optional_ptr<DatabaseInstance> TryGetDatabase() override {
		return &db;
	}
	HTTPUtil &GetHTTPUtil() override {
		return HTTPUtil::Get(*TryGetDatabase());
	}

private:
	DatabaseInstance &db;
	Context &context;
};

class DatabaseFileSystem : public FileSystem {
public:
	explicit DatabaseFileSystem(DatabaseInstance &db_p) : db(db_p), database_opener(db_p) {
	}

private:
	VirtualFileSystem &GetFileSystem() const override {
		return VirtualFileSystem::Get(db);
	}
	optional_ptr<FileOpener> GetOpener() const override {
		return &database_opener;
	}

	DatabaseInstance &db;
	mutable DatabaseFileOpener database_opener;
};

class LocalDatabaseFileSystem : public FileSystem {
public:
	explicit LocalDatabaseFileSystem(DatabaseInstance &db_p);

private:
	VirtualFileSystem &GetFileSystem() const override;
	optional_ptr<FileOpener> GetOpener() const override {
		return &database_opener;
	}
	//! Every path is local: the default file system if it is local, otherwise the owned local file system
	optional_ptr<BaseFileSystem> GetFixedFileSystem() const override;

	VirtualFileSystem &file_system;
	//! Only set if the default file system is not local
	unique_ptr<BaseFileSystem> owned_file_system;
	mutable DatabaseFileOpener database_opener;
};

} // namespace duckdb
