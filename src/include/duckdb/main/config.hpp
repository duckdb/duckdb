//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/cgroups.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/enums/cache_validation_mode.hpp"
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
#include "duckdb/storage/compression/bitpacking.hpp"
#include "duckdb/function/encoding_function.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/user_settings.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/types/type_manager.hpp"

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
class ExtensionCallbackManager;
class TypeManager;

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

//! NOTE: DBConfigOptions is mostly deprecated.
//! If you want to add a setting that can be set by the user, add it as a generic setting to `settings.json`.
//! See e.g. "http_proxy" (HTTPProxySetting) as an example
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
	//! The maximum memory used by the database system (in bytes). Default: 80% of System available memory
	idx_t maximum_memory = DConstants::INVALID_INDEX;
	//! The maximum size of the 'temp_directory' folder when set (in bytes). Default: 90% of available disk space.
	idx_t maximum_swap_space = DConstants::INVALID_INDEX;
	//! The maximum amount of CPU threads used by the database system. Default: all available.
	idx_t maximum_threads = DConstants::INVALID_INDEX;
	//! Whether or not to create and use a temporary directory to store intermediates that do not fit in memory
	bool use_temporary_directory = true;
	//! Directory to store temporary structures that do not fit in memory
	string temporary_directory;
	//! Whether or not to invoke filesystem trim on free blocks after checkpoint. This will reclaim
	//! space for sparse files, on platforms that support it.
	bool trim_free_blocks = false;
	//! Record timestamps of buffer manager unpin() events. Usable by custom eviction policies.
	bool buffer_manager_track_eviction_timestamps = false;
	//! Force checkpoint when CHECKPOINT is called or on shutdown, even if no changes have been made
	bool force_checkpoint = false;
	//! Run a checkpoint on successful shutdown and delete the WAL, to leave only a single database file behind
	bool checkpoint_on_shutdown = true;
	//! Serialize the metadata on checkpoint with compatibility for a given DuckDB version.
	SerializationCompatibility serialization_compatibility = SerializationCompatibility::Default();
	//! Initialize the database with the standard set of DuckDB functions
	//! You should probably not touch this unless you know what you are doing
	bool initialize_default_database = true;
	//! Enable mbedtls explicitly (overrides OpenSSL if available)
	bool force_mbedtls = false;
	//! The set of disabled optimizers (default empty)
	set<OptimizerType> disabled_optimizers;
	//! Force a specific schema for VARIANT shredding
	LogicalType force_variant_shredding = LogicalType::INVALID;
	//! Database configuration variable default values;
	case_insensitive_map_t<Value> set_variable_defaults;
	//! Additional directories to store extension binaries in
	vector<string> extension_directories;
	//! Debug setting - how to initialize  blocks in the storage layer when allocating
	DebugInitialize debug_initialize = DebugInitialize::NO_INITIALIZE;
	//! The set of user-provided options
	case_insensitive_map_t<Value> user_options;
	//! The set of unrecognized (other) options
	case_insensitive_map_t<Value> unrecognized_options;
	//! Whether to print bindings when printing the plan (debug mode only)
	static bool debug_print_bindings; // NOLINT: debug setting
	//! The peak allocation threshold at which to flush the allocator after completing a task (1 << 27, ~128MB)
	idx_t allocator_flush_threshold = 134217728ULL;
	//! If bulk deallocation larger than this occurs, flush outstanding allocations (1 << 30, ~1GB)
	idx_t allocator_bulk_deallocation_flush_threshold = 536870912ULL;
	//! Metadata from DuckDB callers
	string custom_user_agent;
	//! The default block header size for new duckdb database files.
	idx_t default_block_header_size = DEFAULT_BLOCK_HEADER_STORAGE_SIZE;
	//!  Whether or not to abort if a serialization exception is thrown during WAL playback (when reading truncated WAL)
	bool abort_on_wal_failure = false;
	//! Paths that are explicitly allowed, even if enable_external_access is false
	unordered_set<string> allowed_paths;
	//! Directories that are explicitly allowed, even if enable_external_access is false
	set<string> allowed_directories;
	//! The log configuration
	LogConfig log_config = LogConfig();
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

	//! Replacement table scans are automatically attempted when a table name cannot be found in the schema
	vector<ReplacementScan> replacement_scans;

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
	//! Error manager
	unique_ptr<ErrorManager> error_manager;
	//! A reference to the (shared) default allocator (Allocator::DefaultAllocator)
	shared_ptr<Allocator> default_allocator;
	//! A buffer pool can be shared across multiple databases (if desired).
	shared_ptr<BufferPool> buffer_pool;
	//! Provide a custom buffer manager implementation (if desired).
	shared_ptr<BufferManager> buffer_manager;
	//! Encryption Util for OpenSSL and MbedTLS
	shared_ptr<EncryptionUtil> encryption_util;
	//! HTTP Request utility functions
	shared_ptr<HTTPUtil> http_util;
	//! Reference to the database cache entry (if any)
	shared_ptr<DatabaseCacheEntry> db_cache_entry;
	//! Reference to the database file path manager
	shared_ptr<DatabaseFilePathManager> path_manager;
	//! Database configuration variables as controlled by SET
	GlobalUserSettings user_settings;

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
	DUCKDB_API bool HasExtensionOption(const string &name) const;
	DUCKDB_API case_insensitive_map_t<ExtensionOption> GetExtensionSettings() const;
	DUCKDB_API bool TryGetExtensionOption(const String &name, ExtensionOption &result) const;
	//! Fetch an option by index. Returns a pointer to the option, or nullptr if out of range
	DUCKDB_API static optional_ptr<const ConfigurationOption> GetOptionByIndex(idx_t index);
	//! Fetcha n alias by index, or nullptr if out of range
	DUCKDB_API static optional_ptr<const ConfigurationAlias> GetAliasByIndex(idx_t index);
	//! Fetch an option by name. Returns a pointer to the option, or nullptr if none exists.
	DUCKDB_API static optional_ptr<const ConfigurationOption> GetOptionByName(const String &name);
	DUCKDB_API void SetOption(const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option, const Value &value);
	DUCKDB_API void SetOption(const string &name, Value value);
	DUCKDB_API void SetOption(idx_t setting_index, Value value);
	DUCKDB_API void SetOptionByName(const string &name, const Value &value);
	DUCKDB_API void SetOptionsByName(const case_insensitive_map_t<Value> &values);
	DUCKDB_API void ResetOption(optional_ptr<DatabaseInstance> db, const ConfigurationOption &option);
	DUCKDB_API void ResetOption(const ExtensionOption &extension_option);
	DUCKDB_API void ResetGenericOption(idx_t setting_index);
	DUCKDB_API optional_idx TryGetSettingIndex(const String &name,
	                                           optional_ptr<const ConfigurationOption> &option) const;
	static LogicalType ParseLogicalType(const string &type);

	DUCKDB_API void CheckLock(const String &name);

	DUCKDB_API static idx_t ParseMemoryLimit(const string &arg);

	//! Returns the list of possible compression functions for the physical type.
	DUCKDB_API vector<reference<CompressionFunction>> GetCompressionFunctions(const PhysicalType physical_type);
	//! Returns the compression function matching the compression and physical type.
	//! Throws an error if the function does not exist.
	DUCKDB_API reference<CompressionFunction> GetCompressionFunction(CompressionType type,
	                                                                 const PhysicalType physical_type);
	DUCKDB_API optional_ptr<CompressionFunction> TryGetCompressionFunction(CompressionType type,
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
	DUCKDB_API TypeManager &GetTypeManager();
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

	//! Returns the value of a setting currently. If the setting is not set by the user, returns the default value.
	SettingLookupResult TryGetCurrentSetting(const string &key, Value &result) const;
	//! Returns the value of a setting set by the user currently
	SettingLookupResult TryGetCurrentUserSetting(idx_t setting_index, Value &result) const;
	//! Returns the default value of an option
	static SettingLookupResult TryGetDefaultValue(optional_ptr<const ConfigurationOption> option, Value &result);

	bool CanAccessFile(const string &path, FileType type);
	void AddAllowedDirectory(const string &path);
	void AddAllowedPath(const string &path);
	string SanitizeAllowedPath(const string &path) const;
	ExtensionCallbackManager &GetCallbackManager();
	const ExtensionCallbackManager &GetCallbackManager() const;

private:
	mutable mutex config_lock;
	unique_ptr<CompressionFunctionSet> compression_functions;
	unique_ptr<EncodingFunctionSet> encoding_functions;
	unique_ptr<ArrowTypeExtensionSet> arrow_extensions;
	unique_ptr<TypeManager> type_manager;
	unique_ptr<CollationBinding> collation_bindings;
	unique_ptr<IndexTypeSet> index_types;
	unique_ptr<ExtensionCallbackManager> callback_manager;
	bool is_user_config = true;
};

} // namespace duckdb
