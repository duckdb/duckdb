//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/enums/order_type.hpp"

namespace duckdb {
class ClientContext;

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };

// this is optional and only used in tests at the moment
struct DBConfig {
	friend class DuckDB;
	friend class StorageManager;

public:
	~DBConfig();

	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	// Checkpoint when WAL reaches this size
	idx_t checkpoint_wal_size = 1 << 20;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! The maximum memory used by the database system (in bytes). Default: Infinite
	idx_t maximum_memory = (idx_t)-1;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! The collation type of the database
	string collation = string();
	//! The order type used when none is specified (default: ASC)
	OrderType default_order_type = OrderType::ASCENDING;
	//! Null ordering used when none is specified (default: NULLS FIRST)
	OrderByNullType default_null_order = OrderByNullType::NULLS_FIRST;
	//! enable COPY and related commands
	bool enable_copy = true;

public:
	static DBConfig &GetConfig(ClientContext &context);

private:
	// FIXME: don't set this as a user: used internally (only for now)
	bool checkpoint_only = false;
};

} // namespace duckdb
