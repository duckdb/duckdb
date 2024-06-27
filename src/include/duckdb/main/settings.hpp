//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;
struct DBConfig;

const string GetDefaultUserAgent();

enum class SettingScope : uint8_t { GLOBAL, LOCAL, INVALID };

struct SettingLookupResult {
public:
	SettingLookupResult() : scope(SettingScope::INVALID) {
	}
	explicit SettingLookupResult(SettingScope scope) : scope(scope) {
		D_ASSERT(scope != SettingScope::INVALID);
	}

public:
	operator bool() { // NOLINT: allow implicit conversion to bool
		return scope != SettingScope::INVALID;
	}

public:
	SettingScope GetScope() {
		D_ASSERT(scope != SettingScope::INVALID);
		return scope;
	}

private:
	SettingScope scope = SettingScope::INVALID;
};

struct AccessModeSetting {
	static constexpr const char *Name = "access_mode";
	static constexpr const char *Description = "Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllowPersistentSecrets {
	static constexpr const char *Name = "allow_persistent_secrets";
	static constexpr const char *Description =
	    "Allow the creation of persistent secrets, that are stored and loaded on restarts";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct CheckpointThresholdSetting {
	static constexpr const char *Name = "checkpoint_threshold";
	static constexpr const char *Description =
	    "The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DebugCheckpointAbort {
	static constexpr const char *Name = "debug_checkpoint_abort";
	static constexpr const char *Description =
	    "DEBUG SETTING: trigger an abort while checkpointing for testing purposes";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DebugForceExternal {
	static constexpr const char *Name = "debug_force_external";
	static constexpr const char *Description =
	    "DEBUG SETTING: force out-of-core computation for operators that support it, used for testing";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct DebugForceNoCrossProduct {
	static constexpr const char *Name = "debug_force_no_cross_product";
	static constexpr const char *Description =
	    "DEBUG SETTING: Force disable cross product generation when hyper graph isn't connected, used for testing";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct OrderedAggregateThreshold {
	static constexpr const char *Name = "ordered_aggregate_threshold"; // NOLINT
	static constexpr const char *Description =                         // NOLINT
	    "The number of rows to accumulate before sorting, used for tuning";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT; // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct DebugAsOfIEJoin {
	static constexpr const char *Name = "debug_asof_iejoin";                                                 // NOLINT
	static constexpr const char *Description = "DEBUG SETTING: force use of IEJoin to implement AsOf joins"; // NOLINT
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;                                 // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PreferRangeJoins {
	static constexpr const char *Name = "prefer_range_joins";                                    // NOLINT
	static constexpr const char *Description = "Force use of range joins with mixed predicates"; // NOLINT
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;                     // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct DebugWindowMode {
	static constexpr const char *Name = "debug_window_mode";
	static constexpr const char *Description = "DEBUG SETTING: switch window mode to use";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DefaultCollationSetting {
	static constexpr const char *Name = "default_collation";
	static constexpr const char *Description = "The collation setting used when none is specified";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct DefaultOrderSetting {
	static constexpr const char *Name = "default_order";
	static constexpr const char *Description = "The order type used when none is specified (ASC or DESC)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DefaultNullOrderSetting {
	static constexpr const char *Name = "default_null_order";
	static constexpr const char *Description = "Null ordering used when none is specified (NULLS_FIRST or NULLS_LAST)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DefaultSecretStorage {
	static constexpr const char *Name = "default_secret_storage";
	static constexpr const char *Description = "Allows switching the default storage for secrets";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DisabledFileSystemsSetting {
	static constexpr const char *Name = "disabled_filesystems";
	static constexpr const char *Description = "Disable specific file systems preventing access (e.g. LocalFileSystem)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DisabledOptimizersSetting {
	static constexpr const char *Name = "disabled_optimizers";
	static constexpr const char *Description = "DEBUG SETTING: disable a specific set of optimizers (comma separated)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableExternalAccessSetting {
	static constexpr const char *Name = "enable_external_access";
	static constexpr const char *Description =
	    "Allow the database to access external state (through e.g. loading/installing modules, COPY TO/FROM, CSV "
	    "readers, pandas replacement scans, etc)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableMacrosDependencies {
	static constexpr const char *Name = "enable_macro_dependencies";
	static constexpr const char *Description =
	    "Enable created MACROs to create dependencies on the referenced objects (such as tables)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableViewDependencies {
	static constexpr const char *Name = "enable_view_dependencies";
	static constexpr const char *Description =
	    "Enable created VIEWs to create dependencies on the referenced objects (such as tables)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableFSSTVectors {
	static constexpr const char *Name = "enable_fsst_vectors";
	static constexpr const char *Description =
	    "Allow scans on FSST compressed segments to emit compressed vectors to utilize late decompression";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllowUnsignedExtensionsSetting {
	static constexpr const char *Name = "allow_unsigned_extensions";
	static constexpr const char *Description = "Allow to load extensions with invalid or missing signatures";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllowCommunityExtensionsSetting {
	static constexpr const char *Name = "allow_community_extensions";
	static constexpr const char *Description = "Allow to load community built extensions";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllowExtensionsMetadataMismatchSetting {
	static constexpr const char *Name = "allow_extensions_metadata_mismatch";
	static constexpr const char *Description = "Allow to load extensions with not compatible metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllowUnredactedSecretsSetting {
	static constexpr const char *Name = "allow_unredacted_secrets";
	static constexpr const char *Description = "Allow printing unredacted secrets";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct CustomExtensionRepository {
	static constexpr const char *Name = "custom_extension_repository";
	static constexpr const char *Description = "Overrides the custom endpoint for remote extension installation";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AutoloadExtensionRepository {
	static constexpr const char *Name = "autoinstall_extension_repository";
	static constexpr const char *Description =
	    "Overrides the custom endpoint for extension installation on autoloading";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AutoinstallKnownExtensions {
	static constexpr const char *Name = "autoinstall_known_extensions";
	static constexpr const char *Description =
	    "Whether known extensions are allowed to be automatically installed when a query depends on them";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AutoloadKnownExtensions {
	static constexpr const char *Name = "autoload_known_extensions";
	static constexpr const char *Description =
	    "Whether known extensions are allowed to be automatically loaded when a query depends on them";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableObjectCacheSetting {
	static constexpr const char *Name = "enable_object_cache";
	static constexpr const char *Description = "Whether or not object cache is used to cache e.g. Parquet metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct StorageCompatibilityVersion {
	static constexpr const char *Name = "storage_compatibility_version";
	static constexpr const char *Description = "Serialize on checkpoint with compatibility for a given duckdb version";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableHTTPMetadataCacheSetting {
	static constexpr const char *Name = "enable_http_metadata_cache";
	static constexpr const char *Description = "Whether or not the global http metadata is used to cache HTTP metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(const ClientContext &context);
};

struct EnableProfilingSetting {
	static constexpr const char *Name = "enable_profiling";
	static constexpr const char *Description =
	    "Enables profiling, and sets the output format (JSON, QUERY_TREE, QUERY_TREE_OPTIMIZER)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct CustomProfilingSettings {
	static constexpr const char *Name = "custom_profiling_settings";
	static constexpr const char *Description = "Accepts a JSON enabling custom metrics";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct EnableProgressBarSetting {
	static constexpr const char *Name = "enable_progress_bar";
	static constexpr const char *Description =
	    "Enables the progress bar, printing progress to the terminal for long queries";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct EnableProgressBarPrintSetting {
	static constexpr const char *Name = "enable_progress_bar_print";
	static constexpr const char *Description =
	    "Controls the printing of the progress bar, when 'enable_progress_bar' is true";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ErrorsAsJsonSetting {
	static constexpr const char *Name = "errors_as_json";
	static constexpr const char *Description = "Output error messages as structured JSON instead of as a raw string";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ExplainOutputSetting {
	static constexpr const char *Name = "explain_output";
	static constexpr const char *Description = "Output of EXPLAIN statements (ALL, OPTIMIZED_ONLY, PHYSICAL_ONLY)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ExportLargeBufferArrow {
	static constexpr const char *Name = "arrow_large_buffer_size";
	static constexpr const char *Description =
	    "If arrow buffers for strings, blobs, uuids and bits should be exported using large buffers";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ExtensionDirectorySetting {
	static constexpr const char *Name = "extension_directory";
	static constexpr const char *Description = "Set the directory to store extensions in";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ExternalThreadsSetting {
	static constexpr const char *Name = "external_threads";
	static constexpr const char *Description = "The number of external threads that work on DuckDB tasks.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct FileSearchPathSetting {
	static constexpr const char *Name = "file_search_path";
	static constexpr const char *Description = "A comma separated list of directories to search for input files";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ForceCompressionSetting {
	static constexpr const char *Name = "force_compression";
	static constexpr const char *Description = "DEBUG SETTING: forces a specific compression method to be used";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ForceBitpackingModeSetting {
	static constexpr const char *Name = "force_bitpacking_mode";
	static constexpr const char *Description = "DEBUG SETTING: forces a specific bitpacking mode";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct HomeDirectorySetting {
	static constexpr const char *Name = "home_directory";
	static constexpr const char *Description = "Sets the home directory used by the system";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct IntegerDivisionSetting {
	static constexpr const char *Name = "integer_division";
	static constexpr const char *Description =
	    "Whether or not the / operator defaults to integer division, or to floating point division";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct LogQueryPathSetting {
	static constexpr const char *Name = "log_query_path";
	static constexpr const char *Description =
	    "Specifies the path to which queries should be logged (default: NULL, queries are not logged)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct LockConfigurationSetting {
	static constexpr const char *Name = "lock_configuration";
	static constexpr const char *Description = "Whether or not the configuration can be altered";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ImmediateTransactionModeSetting {
	static constexpr const char *Name = "immediate_transaction_mode";
	static constexpr const char *Description =
	    "Whether transactions should be started lazily when needed, or immediately when BEGIN TRANSACTION is called";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct MaximumExpressionDepthSetting {
	static constexpr const char *Name = "max_expression_depth";
	static constexpr const char *Description =
	    "The maximum expression depth limit in the parser. WARNING: increasing this setting and using very deep "
	    "expressions might lead to stack overflow errors.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct MaximumMemorySetting {
	static constexpr const char *Name = "max_memory";
	static constexpr const char *Description = "The maximum memory of the system (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct StreamingBufferSize {
	static constexpr const char *Name = "streaming_buffer_size";
	static constexpr const char *Description =
	    "The maximum memory to buffer between fetching from a streaming result (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct MaximumTempDirectorySize {
	static constexpr const char *Name = "max_temp_directory_size";
	static constexpr const char *Description =
	    "The maximum amount of data stored inside the 'temp_directory' (when set) (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct MergeJoinThreshold {
	static constexpr const char *Name = "merge_join_threshold";
	static constexpr const char *Description = "The number of rows we need on either table to choose a merge join";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct NestedLoopJoinThreshold {
	static constexpr const char *Name = "nested_loop_join_threshold";
	static constexpr const char *Description =
	    "The number of rows we need on either table to choose a nested loop join";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct OldImplicitCasting {
	static constexpr const char *Name = "old_implicit_casting";
	static constexpr const char *Description = "Allow implicit casting to/from VARCHAR";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct PartitionedWriteFlushThreshold {
	static constexpr const char *Name = "partitioned_write_flush_threshold";
	static constexpr const char *Description =
	    "The threshold in number of rows after which we flush a thread state when writing using PARTITION_BY";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PartitionedWriteMaxOpenFiles {
	static constexpr const char *Name = "partitioned_write_max_open_files";
	static constexpr const char *Description =
	    "The maximum amount of files the system can keep open before flushing to disk when writing using PARTITION_BY";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct DefaultBlockAllocSize {
	static constexpr const char *Name = "default_block_size";
	static constexpr const char *Description =
	    "The default block size for new duckdb database files (new as-in, they do not yet exist).";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct IndexScanPercentage {
	static constexpr const char *Name = "index_scan_percentage";
	static constexpr const char *Description =
	    "The index scan percentage sets a threshold for index scans. If fewer than MAX(index_scan_max_count, "
	    "index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::DOUBLE;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct IndexScanMaxCount {
	static constexpr const char *Name = "index_scan_max_count";
	static constexpr const char *Description =
	    "The maximum index scan count sets a threshold for index scans. If fewer than MAX(index_scan_max_count, "
	    "index_scan_percentage * total_row_count) rows match, we perform an index scan instead of a table scan.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct PasswordSetting {
	static constexpr const char *Name = "password";
	static constexpr const char *Description = "The password to use. Ignored for legacy compatibility.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct PerfectHashThresholdSetting {
	static constexpr const char *Name = "perfect_ht_threshold";
	static constexpr const char *Description = "Threshold in bytes for when to use a perfect hash table";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PivotFilterThreshold {
	static constexpr const char *Name = "pivot_filter_threshold";
	static constexpr const char *Description =
	    "The threshold to switch from using filtered aggregates to LIST with a dedicated pivot operator";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PivotLimitSetting {
	static constexpr const char *Name = "pivot_limit";
	static constexpr const char *Description = "The maximum number of pivot columns in a pivot statement";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PreserveIdentifierCase {
	static constexpr const char *Name = "preserve_identifier_case";
	static constexpr const char *Description =
	    "Whether or not to preserve the identifier case, instead of always lowercasing all non-quoted identifiers";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct PreserveInsertionOrder {
	static constexpr const char *Name = "preserve_insertion_order";
	static constexpr const char *Description =
	    "Whether or not to preserve insertion order. If set to false the system is allowed to re-order any results "
	    "that do not contain ORDER BY clauses.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ArrowOutputListView {
	static constexpr const char *Name = "arrow_output_list_view";
	static constexpr const char *Description =
	    "If export to arrow format should use ListView as the physical layout for LIST columns";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ProduceArrowStringView {
	static constexpr const char *Name = "produce_arrow_string_view";
	static constexpr const char *Description =
	    "If strings should be produced by DuckDB in Utf8View format instead of Utf8";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ProfileOutputSetting {
	static constexpr const char *Name = "profile_output";
	static constexpr const char *Description =
	    "The file to which profile output should be saved, or empty to print to the terminal";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ProfilingModeSetting {
	static constexpr const char *Name = "profiling_mode";
	static constexpr const char *Description = "The profiling mode (STANDARD or DETAILED)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct ProgressBarTimeSetting {
	static constexpr const char *Name = "progress_bar_time";
	static constexpr const char *Description =
	    "Sets the time (in milliseconds) how long a query needs to take before we start printing a progress bar";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct SchemaSetting {
	static constexpr const char *Name = "schema";
	static constexpr const char *Description =
	    "Sets the default search schema. Equivalent to setting search_path to a single value.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct SearchPathSetting {
	static constexpr const char *Name = "search_path";
	static constexpr const char *Description =
	    "Sets the default catalog search path as a comma-separated list of values";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct SecretDirectorySetting {
	static constexpr const char *Name = "secret_directory";
	static constexpr const char *Description = "Set the directory to which persistent secrets are stored";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct TempDirectorySetting {
	static constexpr const char *Name = "temp_directory";
	static constexpr const char *Description = "Set the directory to which to write temp files";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct ThreadsSetting {
	static constexpr const char *Name = "threads";
	static constexpr const char *Description = "The number of total threads used by the system.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct UsernameSetting {
	static constexpr const char *Name = "username";
	static constexpr const char *Description = "The username to use. Ignored for legacy compatibility.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct FlushAllocatorSetting {
	static constexpr const char *Name = "allocator_flush_threshold";
	static constexpr const char *Description =
	    "Peak allocation threshold at which to flush the allocator after completing a task.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct AllocatorBackgroundThreadsSetting {
	static constexpr const char *Name = "allocator_background_threads";
	static constexpr const char *Description = "Whether to enable the allocator background thread.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct DuckDBApiSetting {
	static constexpr const char *Name = "duckdb_api";
	static constexpr const char *Description = "DuckDB API surface";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct CustomUserAgentSetting {
	static constexpr const char *Name = "custom_user_agent";
	static constexpr const char *Description = "Metadata from DuckDB callers";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(const ClientContext &context);
};

struct EnableHTTPLoggingSetting {
	static constexpr const char *Name = "enable_http_logging";
	static constexpr const char *Description = "Enables HTTP logging";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

struct HTTPLoggingOutputSetting {
	static constexpr const char *Name = "http_logging_output";
	static constexpr const char *Description =
	    "The file to which HTTP logging output should be saved, or empty to print to the terminal";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(const ClientContext &context);
};

} // namespace duckdb
