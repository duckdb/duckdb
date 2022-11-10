//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/function/replacement_open.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/enums/window_aggregation_mode.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {
class CastFunctionSet;
class ClientContext;
class ErrorManager;
class CompressionFunction;
class TableFunctionRef;

struct CompressionFunctionSet;
struct DBConfig;

enum class AccessMode : uint8_t { UNDEFINED = 0, AUTOMATIC = 1, READ_ONLY = 2, READ_WRITE = 3 };

enum class CheckpointAbort : uint8_t {
	NO_ABORT = 0,
	DEBUG_ABORT_BEFORE_TRUNCATE = 1,
	DEBUG_ABORT_BEFORE_HEADER = 2,
	DEBUG_ABORT_AFTER_FREE_LIST_WRITE = 3
};

typedef void (*set_global_function_t)(DatabaseInstance *db, DBConfig &config, const Value &parameter);
typedef void (*set_local_function_t)(ClientContext &context, const Value &parameter);
typedef Value (*get_setting_function_t)(ClientContext &context);

struct ConfigurationOption {
	const char *name;
	const char *description;
	LogicalTypeId parameter_type;
	set_global_function_t set_global;
	set_local_function_t set_local;
	get_setting_function_t get_setting;
};

typedef void (*set_option_callback_t)(ClientContext &context, SetScope scope, Value &parameter);

struct ExtensionOption {
	ExtensionOption(string description_p, LogicalType type_p, set_option_callback_t set_function_p)
	    : description(move(description_p)), type(move(type_p)), set_function(set_function_p) {
	}

	string description;
	LogicalType type;
	set_option_callback_t set_function;
};

struct DBConfigOptions {
	//! Database file path. May be empty for in-memory mode
	string database_path;
	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	//! Checkpoint when WAL reaches this size (default: 16MB)
	idx_t checkpoint_wal_size = 1 << 24;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! Whether extensions should be loaded on start-up
	bool load_extensions = true;
	//! The maximum memory used by the database system (in bytes). Default: 80% of System available memory
	idx_t maximum_memory = (idx_t)-1;
	//! The maximum amount of CPU threads used by the database system. Default: all available.
	idx_t maximum_threads = (idx_t)-1;
	//! The number of external threads that work on DuckDB tasks. Default: none.
	idx_t external_threads = 0;
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
	bool enable_external_access = true;
	//! Whether or not object cache is used
	bool object_cache_enable = false;
	//! Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
	bool force_checkpoint = false;
	//! Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
	bool checkpoint_on_shutdown = true;
	//! Debug flag that decides when a checkpoing should be aborted. Only used for testing purposes.
	CheckpointAbort checkpoint_abort = CheckpointAbort::NO_ABORT;
	//! Initialize the database with the standard set of DuckDB functions
	//! You should probably not touch this unless you know what you are doing
	bool initialize_default_database = true;
	//! The set of disabled optimizers (default empty)
	set<OptimizerType> disabled_optimizers;
	//! Force a specific compression method to be used when checkpointing (if available)
	CompressionType force_compression = CompressionType::COMPRESSION_AUTO;
	//! Debug setting for window aggregation mode: (window, combine, separate)
	WindowAggregationMode window_mode = WindowAggregationMode::WINDOW;
	//! Whether or not preserving insertion order should be preserved
	bool preserve_insertion_order = true;
	//! Database configuration variables as controlled by SET
	case_insensitive_map_t<Value> set_variables;
	//! Whether unsigned extensions should be loaded
	bool allow_unsigned_extensions = false;
	//! Enable emitting FSST Vectors
	bool enable_fsst_vectors = false;

	bool operator==(const DBConfigOptions &other) const;
};

struct DBConfig {
	friend class DatabaseInstance;
	friend class StorageManager;

public:
	DUCKDB_API DBConfig();
	DUCKDB_API DBConfig(std::unordered_map<string, string> &config_dict, bool read_only);
	DUCKDB_API ~DBConfig();

	mutex config_lock;
	//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
	vector<ReplacementScan> replacement_scans;

	//! Replacement open handlers are callbacks that run pre and post database initialization
	vector<ReplacementOpen> replacement_opens;

	//! Extra parameters that can be SET for loaded extensions
	case_insensitive_map_t<ExtensionOption> extension_parameters;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! The allocator used by the system
	unique_ptr<Allocator> allocator;
	//! Database configuration options
	DBConfigOptions options;
	//! Extensions made to the parser
	vector<ParserExtension> parser_extensions;
	//! Extensions made to the optimizer
	vector<OptimizerExtension> optimizer_extensions;
	//! Error manager
	unique_ptr<ErrorManager> error_manager;

public:
	DUCKDB_API static DBConfig &GetConfig(ClientContext &context);
	DUCKDB_API static DBConfig &GetConfig(DatabaseInstance &db);
	DUCKDB_API static const DBConfig &GetConfig(const ClientContext &context);
	DUCKDB_API static const DBConfig &GetConfig(const DatabaseInstance &db);
	DUCKDB_API static vector<ConfigurationOption> GetOptions();
	DUCKDB_API static idx_t GetOptionCount();
	DUCKDB_API static vector<string> GetOptionNames();

	DUCKDB_API void AddExtensionOption(string name, string description, LogicalType parameter,
	                                   set_option_callback_t function = nullptr);
	//! Fetch an option by index. Returns a pointer to the option, or nullptr if out of range
	DUCKDB_API static ConfigurationOption *GetOptionByIndex(idx_t index);
	//! Fetch an option by name. Returns a pointer to the option, or nullptr if none exists.
	DUCKDB_API static ConfigurationOption *GetOptionByName(const string &name);

	DUCKDB_API void SetOption(const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOption(DatabaseInstance *db, const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOption(const string &name, Value value);

	DUCKDB_API static idx_t ParseMemoryLimit(const string &arg);

	//! Return the list of possible compression functions for the specific physical type
	DUCKDB_API vector<CompressionFunction *> GetCompressionFunctions(PhysicalType data_type);
	//! Return the compression function for the specified compression type/physical type combo
	DUCKDB_API CompressionFunction *GetCompressionFunction(CompressionType type, PhysicalType data_type);

	bool operator==(const DBConfig &other);
	bool operator!=(const DBConfig &other);

	DUCKDB_API CastFunctionSet &GetCastFunctions();

private:
	unique_ptr<CompressionFunctionSet> compression_functions;
	unique_ptr<CastFunctionSet> cast_functions;
};

} // namespace duckdb
