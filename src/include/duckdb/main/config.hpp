//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_type_extension.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/cgroups.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/encryption_state.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/enums/thread_pin_mode.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/enums/set_scope.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/storage/compression/bitpacking.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/logging/log_manager.hpp"

namespace duckdb {

class BlockAllocator;
class BufferManager;
class BufferPool;
class CastFunctionSet;
class CollationBinding;
class ClientContext;
class ErrorManager;
class CompressionFunction;
class TableFunctionRef;
class OperatorExtension;
class StorageExtension;
class ExtensionCallback;
class SecretManager;
class CompressionInfo;
class EncryptionUtil;
class HTTPUtil;
class DatabaseFilePathManager;

struct CompressionFunctionSet;
struct DatabaseCacheEntry;
struct DBConfig;
struct SettingLookupResult;

class SerializationCompatibility {
public:
	static SerializationCompatibility FromDatabase(AttachedDatabase &db);
	static SerializationCompatibility FromIndex(idx_t serialization_version);
	static SerializationCompatibility FromString(const string &input);
	static SerializationCompatibility Default();
	static SerializationCompatibility Latest();

public:
	bool Compare(idx_t property_version) const;

public:
	//! The user provided version
	string duckdb_version;
	//! The max version that should be serialized
	idx_t serialization_version;
	//! Whether this was set by a manual SET/PRAGMA or default
	bool manually_set;

protected:
	SerializationCompatibility() = default;
};

struct DBConfigOptions {
	//! Database file path. May be empty for in-memory mode
	string database_path;
	//! Database type. If empty, automatically extracted from `database_path`, where a `type:path` syntax is expected
	string database_type;
	//! Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)
	AccessMode access_mode = AccessMode::AUTOMATIC;
	//! Checkpoint when WAL reaches this size (default: 16MB)
	idx_t checkpoint_wal_size = 1 << 24;
	//! Whether or not to use Direct IO, bypassing operating system buffers
	bool use_direct_io = false;
	//! Whether extensions should be loaded on start-up
	bool load_extensions = true;
#ifdef DUCKDB_EXTENSION_AUTOLOAD_DEFAULT
	//! Whether known extensions are allowed to be automatically loaded when a query depends on them
	bool autoload_known_extensions = DUCKDB_EXTENSION_AUTOLOAD_DEFAULT;
#else
	bool autoload_known_extensions = false;
#endif
#ifdef DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT
	//! Whether known extensions are allowed to be automatically installed when a query depends on them
	bool autoinstall_known_extensions = DUCKDB_EXTENSION_AUTOINSTALL_DEFAULT;
#else
	bool autoinstall_known_extensions = false;
#endif
	//! Setting for the parser override registered by extensions. Allowed options: "default, "fallback", "strict"
	string allow_parser_override_extension = "default";
	//! Override for the default extension repository
	string custom_extension_repo = "";
	//! Override for the default autoload extension repository
	string autoinstall_extension_repo = "";
	//! The maximum memory used by the database system (in bytes). Default: 80% of System available memory
	idx_t maximum_memory = DConstants::INVALID_INDEX;
	//! The maximum size of the 'temp_directory' folder when set (in bytes). Default: 90% of available disk space.
	idx_t maximum_swap_space = DConstants::INVALID_INDEX;
	//! The maximum amount of CPU threads used by the database system. Default: all available.
	idx_t maximum_threads = DConstants::INVALID_INDEX;
	//! The number of external threads that work on DuckDB tasks. Default: 1.
	//! Must be smaller or equal to maximum_threads.
	idx_t external_threads = 1;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim
	//! space for sparse files, on platforms that support it.
	bool trim_free_blocks = false;
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool buffer_manager_track_eviction_timestamps = false;
	//! Whether or not to allow printing unredacted secrets
	bool allow_unredacted_secrets = false;
	//! Disables invalidating the database instance when encountering a fatal error.
	bool disable_database_invalidation = false;
	//! enable COPY and related commands
	bool enable_external_access = true;
	//! Whether or not the global http metadata cache is used
	bool http_metadata_cache_enable = false;
	//! HTTP Proxy config as 'hostname:port'
	string http_proxy;
	//! HTTP Proxy username for basic auth
	string http_proxy_username;
	//! HTTP Proxy password for basic auth
	string http_proxy_password;
	//! Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
	bool force_checkpoint = false;
	//! Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
	bool checkpoint_on_shutdown = true;
	//! Serialize the metadata on checkpoint with compatibility for a given DuckDB version.
	SerializationCompatibility serialization_compatibility = SerializationCompatibility::Default();
	//! Initialize the database with the standard set of DuckDB functions
	//! You should probably not touch this unless you know what you are doing
	bool initialize_default_database = true;
	//! The set of disabled optimizers (default empty)
	set<OptimizerType> disabled_optimizers;
	//! The average string length required to use ZSTD compression.
	uint64_t zstd_min_string_length = 4096;
	//! Force a specific compression method to be used when checkpointing (if available)
	CompressionType force_compression = CompressionType::COMPRESSION_AUTO;
	//! Force a specific bitpacking mode to be used when using the bitpacking compression method
	BitpackingMode force_bitpacking_mode = BitpackingMode::AUTO;
	//! Database configuration variables as controlled by SET
	case_insensitive_map_t<Value> set_variables;
	//! Database configuration variable default values;
	case_insensitive_map_t<Value> set_variable_defaults;
	//! Directory to store extension binaries in
	string extension_directory;
	//! Whether unsigned extensions should be loaded
	bool allow_unsigned_extensions = false;
	//! Whether community extensions should be loaded
	bool allow_community_extensions = true;
	//! Debug setting - how to initialize  blocks in the storage layer when allocating
	DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
	//! The set of user-provided options
	case_insensitive_map_t<Value> user_options;
	//! The set of unrecognized (other) options
	case_insensitive_map_t<Value> unrecognized_options;
	//! Whether or not the configuration settings can be altered
	bool lock_configuration = false;
	//! Whether to print bindings when printing the plan (debug mode only)
	static bool debug_print_bindings; // NOLINT: debug setting
	//! The peak allocation threshold at which to flush the allocator after completing a task (1 << 27, ~128MB)
	idx_t allocator_flush_threshold = 134217728ULL;
	//! If bulk deallocation larger than this occurs, flush outstanding allocations (1 << 30, ~1GB)
	idx_t allocator_bulk_deallocation_flush_threshold = 536870912ULL;
	//! Whether the allocator background thread is enabled
	bool allocator_background_threads = false;
	//! DuckDB API surface
	string duckdb_api;
	//! Metadata from DuckDB callers
	string custom_user_agent;
	//! Encrypt the temp files
	bool temp_file_encryption = false;
	//! The default block allocation size for new duckdb database files (new as-in, they do not yet exist).
	idx_t default_block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE;
	//! The default block header size for new duckdb database files.
	idx_t default_block_header_size = DUCKDB_BLOCK_HEADER_STORAGE_SIZE;
	//!  Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL)
	bool abort_on_wal_failure = false;
	//! Paths that are explicitly allowed, even if enable_external_access is false
	unordered_set<string> allowed_paths;
	//! Directories that are explicitly allowed, even if enable_external_access is false
	set<string> allowed_directories;
	//! The log configuration
	LogConfig log_config = LogConfig();
	//! Whether to enable external file caching using CachingFileSystem
	bool enable_external_file_cache = true;
	//! Partially process tasks before rescheduling - allows for more scheduler fairness between separate queries
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	bool scheduler_process_partial = true;
#else
	bool scheduler_process_partial = false;
#endif
	//! Whether to pin threads to cores (linux only, default AUTOMATIC: on when there are more than 64 cores)
	ThreadPinMode pin_threads = ThreadPinMode::AUTO;
	//! Physical memory that the block allocator is allowed to use (this memory is never freed and cannot be reduced)
	idx_t block_allocator_size = 0;

	bool operator==(const DBConfigOptions &other) const;
};

struct DBConfig {
	friend class DatabaseInstance;
	friend class StorageManager;

public:
	DUCKDB_API DBConfig();
	explicit DUCKDB_API DBConfig(bool read_only);
	DUCKDB_API DBConfig(const case_insensitive_map_t<Value> &config_dict, bool read_only);
	DUCKDB_API ~DBConfig();

	mutable mutex config_lock;
	//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
	vector<ReplacementScan> replacement_scans;

	//! Extra parameters that can be SET for loaded extensions
	case_insensitive_map_t<ExtensionOption> extension_parameters;
	//! The FileSystem to use, can be overwritten to allow for injecting custom file systems for testing purposes (e.g.
	//! RamFS or something similar)
	unique_ptr<FileSystem> file_system;
	//! Secret manager
	unique_ptr<SecretManager> secret_manager;
	//! The allocator used by the system
	unique_ptr<Allocator> allocator;
	//! The block allocator used by the system
	unique_ptr<BlockAllocator> block_allocator;
	//! Database configuration options
	DBConfigOptions options;
	//! Extensions made to the parser
	vector<ParserExtension> parser_extensions;
	//! Extensions made to the optimizer
	vector<OptimizerExtension> optimizer_extensions;
	//! Error manager
	unique_ptr<ErrorManager> error_manager;
	//! A reference to the (shared) default allocator (Allocator::DefaultAllocator)
	shared_ptr<Allocator> default_allocator;
	//! Extensions made to binder
	vector<unique_ptr<OperatorExtension>> operator_extensions;
	//! Extensions made to storage
	case_insensitive_map_t<duckdb::unique_ptr<StorageExtension>> storage_extensions;
	//! A buffer pool can be shared across multiple databases (if desired).
	shared_ptr<BufferPool> buffer_pool;
	//! Provide a custom buffer manager implementation (if desired).
	shared_ptr<BufferManager> buffer_manager;
	//! Set of callbacks that can be installed by extensions
	vector<unique_ptr<ExtensionCallback>> extension_callbacks;
	//! Encryption Util for OpenSSL
	shared_ptr<EncryptionUtil> encryption_util;
	//! HTTP Request utility functions
	shared_ptr<HTTPUtil> http_util;
	//! Reference to the database cache entry (if any)
	shared_ptr<DatabaseCacheEntry> db_cache_entry;
	//! Reference to the database file path manager
	shared_ptr<DatabaseFilePathManager> path_manager;

public:
	DUCKDB_API static DBConfig &GetConfig(ClientContext &context);
	DUCKDB_API static DBConfig &GetConfig(DatabaseInstance &db);
	DUCKDB_API static DBConfig &Get(AttachedDatabase &db);
	DUCKDB_API static const DBConfig &GetConfig(const ClientContext &context);
	DUCKDB_API static const DBConfig &GetConfig(const DatabaseInstance &db);
	DUCKDB_API static vector<ConfigurationOption> GetOptions();
	DUCKDB_API static vector<ConfigurationAlias> GetAliases();
	DUCKDB_API static idx_t GetOptionCount();
	DUCKDB_API static idx_t GetAliasCount();
	DUCKDB_API static vector<string> GetOptionNames();
	DUCKDB_API static bool IsInMemoryDatabase(const char *database_path);

	DUCKDB_API void AddExtensionOption(const string &name, string description, LogicalType parameter,
	                                   const Value &default_value = Value(), set_option_callback_t function = nullptr,
	                                   SetScope default_scope = SetScope::SESSION);
	DUCKDB_API bool HasExtensionOption(const string &name);
	//! Fetch an option by index. Returns a pointer to the option, or nullptr if out of range
	DUCKDB_API static optional_ptr<const ConfigurationOption> GetOptionByIndex(idx_t index);
	//! Fetcha n alias by index, or nullptr if out of range
	DUCKDB_API static optional_ptr<const ConfigurationAlias> GetAliasByIndex(idx_t index);
	//! Fetch an option by name. Returns a pointer to the option, or nullptr if none exists.
	DUCKDB_API static optional_ptr<const ConfigurationOption> GetOptionByName(const String &name);
	DUCKDB_API void SetOption(const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOptionByName(const string &name, const Value &value);
	DUCKDB_API void SetOptionsByName(const case_insensitive_map_t<Value> &values);
	DUCKDB_API void ResetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option);
	DUCKDB_API void SetOption(const String &name, Value value);
	DUCKDB_API void ResetOption(const String &name);
	DUCKDB_API void ResetGenericOption(const String &name);
	static LogicalType ParseLogicalType(const string &type);

	DUCKDB_API void CheckLock(const String &name);

	DUCKDB_API static idx_t ParseMemoryLimit(const string &arg);

	//! Returns the list of possible compression functions for the physical type.
	DUCKDB_API vector<reference<CompressionFunction>> GetCompressionFunctions(const PhysicalType physical_type);
	//! Returns the compression function matching the compression and physical type.
	DUCKDB_API optional_ptr<CompressionFunction> GetCompressionFunction(CompressionType type,
	                                                                    const PhysicalType physical_type);
	//! Sets the disabled compression methods
	DUCKDB_API void SetDisabledCompressionMethods(const vector<CompressionType> &disabled_compression_methods);
	//! Returns a list of disabled compression methods
	DUCKDB_API vector<CompressionType> GetDisabledCompressionMethods() const;

	//! Returns the encode function matching the encoding name.
	DUCKDB_API optional_ptr<EncodingFunction> GetEncodeFunction(const string &name) const;
	DUCKDB_API void RegisterEncodeFunction(const EncodingFunction &function) const;
	//! Returns the encode function names.
	DUCKDB_API vector<reference<EncodingFunction>> GetLoadedEncodedFunctions() const;
	//! Returns the encode function matching the encoding name.
	DUCKDB_API ArrowTypeExtension GetArrowExtension(ArrowExtensionMetadata info) const;
	DUCKDB_API ArrowTypeExtension GetArrowExtension(const LogicalType &type) const;
	DUCKDB_API bool HasArrowExtension(const LogicalType &type) const;
	DUCKDB_API bool HasArrowExtension(ArrowExtensionMetadata info) const;
	DUCKDB_API void RegisterArrowExtension(const ArrowTypeExtension &extension) const;

	bool operator==(const DBConfig &other);
	bool operator!=(const DBConfig &other);

	DUCKDB_API CastFunctionSet &GetCastFunctions();
	DUCKDB_API CollationBinding &GetCollationBinding();
	DUCKDB_API IndexTypeSet &GetIndexTypes();
	static idx_t GetSystemMaxThreads(FileSystem &fs);
	static idx_t GetSystemAvailableMemory(FileSystem &fs);
	static optional_idx ParseMemoryLimitSlurm(const string &arg);
	void SetDefaultMaxMemory();
	void SetDefaultTempDirectory();

	OrderType ResolveOrder(ClientContext &context, OrderType order_type) const;
	OrderByNullType ResolveNullOrder(ClientContext &context, OrderType order_type, OrderByNullType null_type) const;
	const string UserAgent() const;

	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) const;

	template <class OP, class SOURCE>
	static typename std::enable_if<std::is_enum<typename OP::RETURN_TYPE>::value, typename OP::RETURN_TYPE>::type
	GetSetting(const SOURCE &source) {
		return EnumUtil::FromString<typename OP::RETURN_TYPE>(
		    GetSettingInternal(source, OP::Name, OP::DefaultValue).ToString());
	}

	template <class OP, class SOURCE>
	static typename std::enable_if<!std::is_enum<typename OP::RETURN_TYPE>::value, typename OP::RETURN_TYPE>::type
	GetSetting(const SOURCE &source) {
		return GetSettingInternal(source, OP::Name, OP::DefaultValue).template GetValue<typename OP::RETURN_TYPE>();
	}

	template <class OP>
	Value GetSettingValue(const ClientContext &context) const {
		lock_guard<mutex> lock(config_lock);
		return OP::GetSetting(context);
	}

	bool CanAccessFile(const string &path, FileType type);
	void AddAllowedDirectory(const string &path);
	void AddAllowedPath(const string &path);
	string SanitizeAllowedPath(const string &path) const;

private:
	static Value GetSettingInternal(const DatabaseInstance &db, const char *setting, const char *default_value);
	static Value GetSettingInternal(const DBConfig &config, const char *setting, const char *default_value);
	static Value GetSettingInternal(const ClientContext &context, const char *setting, const char *default_value);

private:
	unique_ptr<CompressionFunctionSet> compression_functions;
	unique_ptr<EncodingFunctionSet> encoding_functions;
	unique_ptr<ArrowTypeExtensionSet> arrow_extensions;
	unique_ptr<CastFunctionSet> cast_functions;
	unique_ptr<CollationBinding> collation_bindings;
	unique_ptr<IndexTypeSet> index_types;
	bool is_user_config = true;
};

} // namespace duckdb
