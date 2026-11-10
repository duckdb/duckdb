//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_file_system.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/http/http_util.hpp"


namespace duckdb {
class DatabaseInstance;

//! The file system of a database: bound to the database-wide context
class DatabaseFileSystem : public FileSystem {
public:
	explicit DatabaseFileSystem(DatabaseInstance &db_p) : db(db_p), context(db_p.GetContext()) {
	}

private:
	VirtualFileSystem &GetFileSystem() const override {
		return VirtualFileSystem::Get(db);
	}
	Context &GetContext() const override {
		return context;
	}

	DatabaseInstance &db;
	Context &context;
};

//! The local file system of a database: bound to the database-wide context
class LocalDatabaseFileSystem : public FileSystem {
public:
	explicit LocalDatabaseFileSystem(DatabaseInstance &db_p);

private:
	VirtualFileSystem &GetFileSystem() const override;
	Context &GetContext() const override {
		return context;
	}
	//! Every path is local: the default file system if it is local, otherwise the owned local file system
	optional_ptr<BaseFileSystem> GetFixedFileSystem() const override;

	VirtualFileSystem &file_system;
	//! Only set if the default file system is not local
	unique_ptr<BaseFileSystem> owned_file_system;
	Context &context;
};

} // namespace duckdb
