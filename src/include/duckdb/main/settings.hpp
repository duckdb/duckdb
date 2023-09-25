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

struct AccessModeSetting {
	static constexpr const char *Name = "access_mode";
	static constexpr const char *Description = "Access mode of the database (AUTOMATIC, READ_ONLY or READ_WRITE)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct CheckpointThresholdSetting {
	static constexpr const char *Name = "checkpoint_threshold";
	static constexpr const char *Description =
	    "The WAL size threshold at which to automatically trigger a checkpoint (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DebugCheckpointAbort {
	static constexpr const char *Name = "debug_checkpoint_abort";
	static constexpr const char *Description =
	    "DEBUG SETTING: trigger an abort while checkpointing for testing purposes";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DebugForceExternal {
	static constexpr const char *Name = "debug_force_external";
	static constexpr const char *Description =
	    "DEBUG SETTING: force out-of-core computation for operators that support it, used for testing";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct DebugForceNoCrossProduct {
	static constexpr const char *Name = "debug_force_no_cross_product";
	static constexpr const char *Description =
	    "DEBUG SETTING: Force disable cross product generation when hyper graph isn't connected, used for testing";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct OrderedAggregateThreshold {
	static constexpr const char *Name = "ordered_aggregate_threshold"; // NOLINT
	static constexpr const char *Description =                         // NOLINT
	    "The number of rows to accumulate before sorting, used for tuning";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT; // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct DebugAsOfIEJoin {
	static constexpr const char *Name = "debug_asof_iejoin";                                                 // NOLINT
	static constexpr const char *Description = "DEBUG SETTING: force use of IEJoin to implement AsOf joins"; // NOLINT
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;                                 // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct PreferRangeJoins {
	static constexpr const char *Name = "prefer_range_joins";                                    // NOLINT
	static constexpr const char *Description = "Force use of range joins with mixed predicates"; // NOLINT
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;                     // NOLINT
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct DebugWindowMode {
	static constexpr const char *Name = "debug_window_mode";
	static constexpr const char *Description = "DEBUG SETTING: switch window mode to use";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DefaultCollationSetting {
	static constexpr const char *Name = "default_collation";
	static constexpr const char *Description = "The collation setting used when none is specified";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct DefaultOrderSetting {
	static constexpr const char *Name = "default_order";
	static constexpr const char *Description = "The order type used when none is specified (ASC or DESC)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DefaultNullOrderSetting {
	static constexpr const char *Name = "default_null_order";
	static constexpr const char *Description = "Null ordering used when none is specified (NULLS_FIRST or NULLS_LAST)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DisabledFileSystemsSetting {
	static constexpr const char *Name = "disabled_filesystems";
	static constexpr const char *Description = "Disable specific file systems preventing access (e.g. LocalFileSystem)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct DisabledOptimizersSetting {
	static constexpr const char *Name = "disabled_optimizers";
	static constexpr const char *Description = "DEBUG SETTING: disable a specific set of optimizers (comma separated)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct EnableExternalAccessSetting {
	static constexpr const char *Name = "enable_external_access";
	static constexpr const char *Description =
	    "Allow the database to access external state (through e.g. loading/installing modules, COPY TO/FROM, CSV "
	    "readers, pandas replacement scans, etc)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct EnableFSSTVectors {
	static constexpr const char *Name = "enable_fsst_vectors";
	static constexpr const char *Description =
	    "Allow scans on FSST compressed segments to emit compressed vectors to utilize late decompression";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct AllowUnsignedExtensionsSetting {
	static constexpr const char *Name = "allow_unsigned_extensions";
	static constexpr const char *Description = "Allow to load extensions with invalid or missing signatures";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct CustomExtensionRepository {
	static constexpr const char *Name = "custom_extension_repository";
	static constexpr const char *Description = "Overrides the custom endpoint for remote extension installation";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct AutoloadExtensionRepository {
	static constexpr const char *Name = "autoinstall_extension_repository";
	static constexpr const char *Description =
	    "Overrides the custom endpoint for extension installation on autoloading";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct AutoinstallKnownExtensions {
	static constexpr const char *Name = "autoinstall_known_extensions";
	static constexpr const char *Description =
	    "Whether known extensions are allowed to be automatically installed when a query depends on them";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct AutoloadKnownExtensions {
	static constexpr const char *Name = "autoload_known_extensions";
	static constexpr const char *Description =
	    "Whether known extensions are allowed to be automatically loaded when a query depends on them";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct EnableObjectCacheSetting {
	static constexpr const char *Name = "enable_object_cache";
	static constexpr const char *Description = "Whether or not object cache is used to cache e.g. Parquet metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct EnableHTTPMetadataCacheSetting {
	static constexpr const char *Name = "enable_http_metadata_cache";
	static constexpr const char *Description = "Whether or not the global http metadata is used to cache HTTP metadata";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static Value GetSetting(ClientContext &context);
};

struct EnableProfilingSetting {
	static constexpr const char *Name = "enable_profiling";
	static constexpr const char *Description =
	    "Enables profiling, and sets the output format (JSON, QUERY_TREE, QUERY_TREE_OPTIMIZER)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct EnableProgressBarSetting {
	static constexpr const char *Name = "enable_progress_bar";
	static constexpr const char *Description =
	    "Enables the progress bar, printing progress to the terminal for long queries";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct EnableProgressBarPrintSetting {
	static constexpr const char *Name = "enable_progress_bar_print";
	static constexpr const char *Description =
	    "Controls the printing of the progress bar, when 'enable_progress_bar' is true";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ExplainOutputSetting {
	static constexpr const char *Name = "explain_output";
	static constexpr const char *Description = "Output of EXPLAIN statements (ALL, OPTIMIZED_ONLY, PHYSICAL_ONLY)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ExtensionDirectorySetting {
	static constexpr const char *Name = "extension_directory";
	static constexpr const char *Description = "Set the directory to store extensions in";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ExternalThreadsSetting {
	static constexpr const char *Name = "external_threads";
	static constexpr const char *Description = "The number of external threads that work on DuckDB tasks.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct FileSearchPathSetting {
	static constexpr const char *Name = "file_search_path";
	static constexpr const char *Description = "A comma separated list of directories to search for input files";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ForceCompressionSetting {
	static constexpr const char *Name = "force_compression";
	static constexpr const char *Description = "DEBUG SETTING: forces a specific compression method to be used";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ForceBitpackingModeSetting {
	static constexpr const char *Name = "force_bitpacking_mode";
	static constexpr const char *Description = "DEBUG SETTING: forces a specific bitpacking mode";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct HomeDirectorySetting {
	static constexpr const char *Name = "home_directory";
	static constexpr const char *Description = "Sets the home directory used by the system";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct IntegerDivisionSetting {
	static constexpr const char *Name = "integer_division";
	static constexpr const char *Description =
	    "Whether or not the / operator defaults to integer division, or to floating point division";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct LogQueryPathSetting {
	static constexpr const char *Name = "log_query_path";
	static constexpr const char *Description =
	    "Specifies the path to which queries should be logged (default: empty string, queries are not logged)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct LockConfigurationSetting {
	static constexpr const char *Name = "lock_configuration";
	static constexpr const char *Description = "Whether or not the configuration can be altered";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ImmediateTransactionModeSetting {
	static constexpr const char *Name = "immediate_transaction_mode";
	static constexpr const char *Description =
	    "Whether transactions should be started lazily when needed, or immediately when BEGIN TRANSACTION is called";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct MaximumExpressionDepthSetting {
	static constexpr const char *Name = "max_expression_depth";
	static constexpr const char *Description =
	    "The maximum expression depth limit in the parser. WARNING: increasing this setting and using very deep "
	    "expressions might lead to stack overflow errors.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::UBIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct MaximumMemorySetting {
	static constexpr const char *Name = "max_memory";
	static constexpr const char *Description = "The maximum memory of the system (e.g. 1GB)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct PasswordSetting {
	static constexpr const char *Name = "password";
	static constexpr const char *Description = "The password to use. Ignored for legacy compatibility.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct PerfectHashThresholdSetting {
	static constexpr const char *Name = "perfect_ht_threshold";
	static constexpr const char *Description = "Threshold in bytes for when to use a perfect hash table (default: 12)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct PivotFilterThreshold {
	static constexpr const char *Name = "pivot_filter_threshold";
	static constexpr const char *Description =
	    "The threshold to switch from using filtered aggregates to LIST with a dedicated pivot operator";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct PivotLimitSetting {
	static constexpr const char *Name = "pivot_limit";
	static constexpr const char *Description =
	    "The maximum number of pivot columns in a pivot statement (default: 100000)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct PreserveIdentifierCase {
	static constexpr const char *Name = "preserve_identifier_case";
	static constexpr const char *Description =
	    "Whether or not to preserve the identifier case, instead of always lowercasing all non-quoted identifiers";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct PreserveInsertionOrder {
	static constexpr const char *Name = "preserve_insertion_order";
	static constexpr const char *Description =
	    "Whether or not to preserve insertion order. If set to false the system is allowed to re-order any results "
	    "that do not contain ORDER BY clauses.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ExportLargeBufferArrow {
	static constexpr const char *Name = "arrow_large_buffer_size";
	static constexpr const char *Description =
	    "If arrow buffers for strings, blobs, uuids and bits should be exported using large buffers";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BOOLEAN;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ProfilerHistorySize {
	static constexpr const char *Name = "profiler_history_size";
	static constexpr const char *Description = "Sets the profiler history size";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ProfileOutputSetting {
	static constexpr const char *Name = "profile_output";
	static constexpr const char *Description =
	    "The file to which profile output should be saved, or empty to print to the terminal";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ProfilingModeSetting {
	static constexpr const char *Name = "profiling_mode";
	static constexpr const char *Description = "The profiling mode (STANDARD or DETAILED)";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct ProgressBarTimeSetting {
	static constexpr const char *Name = "progress_bar_time";
	static constexpr const char *Description =
	    "Sets the time (in milliseconds) how long a query needs to take before we start printing a progress bar";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct SchemaSetting {
	static constexpr const char *Name = "schema";
	static constexpr const char *Description =
	    "Sets the default search schema. Equivalent to setting search_path to a single value.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct SearchPathSetting {
	static constexpr const char *Name = "search_path";
	static constexpr const char *Description =
	    "Sets the default catalog search path as a comma-separated list of values";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetLocal(ClientContext &context, const Value &parameter);
	static void ResetLocal(ClientContext &context);
	static Value GetSetting(ClientContext &context);
};

struct TempDirectorySetting {
	static constexpr const char *Name = "temp_directory";
	static constexpr const char *Description = "Set the directory to which to write temp files";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct ThreadsSetting {
	static constexpr const char *Name = "threads";
	static constexpr const char *Description = "The number of total threads used by the system.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::BIGINT;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct UsernameSetting {
	static constexpr const char *Name = "username";
	static constexpr const char *Description = "The username to use. Ignored for legacy compatibility.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

struct FlushAllocatorSetting {
	static constexpr const char *Name = "allocator_flush_threshold";
	static constexpr const char *Description =
	    "Peak allocation threshold at which to flush the allocator after completing a task.";
	static constexpr const LogicalTypeId InputType = LogicalTypeId::VARCHAR;
	static void SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter);
	static void ResetGlobal(DatabaseInstance *db, DBConfig &config);
	static Value GetSetting(ClientContext &context);
};

} // namespace duckdb
