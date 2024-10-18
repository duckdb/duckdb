//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
class DatabaseInstance;

class DatabaseFileOpener : public FileOpener {
public:
	explicit DatabaseFileOpener(DatabaseInstance &db_p) : db(db_p) {
	}

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) override {
		return db.TryGetCurrentSetting(key, result);
	}

	optional_ptr<ClientContext> TryGetClientContext() override {
		return nullptr;
	}

	optional_ptr<DatabaseInstance> TryGetDatabase() override {
		return &db;
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

} // namespace duckdb
