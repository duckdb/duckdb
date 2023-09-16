#include "duckdb/main/settings.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void AccessModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "automatic") {
		config.options.access_mode = AccessMode::AUTOMATIC;
	} else if (parameter == "read_only") {
		config.options.access_mode = AccessMode::READ_ONLY;
	} else if (parameter == "read_write") {
		config.options.access_mode = AccessMode::READ_WRITE;
	} else {
		throw InvalidInputException(
		    "Unrecognized parameter for option ACCESS_MODE \"%s\". Expected READ_ONLY or READ_WRITE.", parameter);
	}
}

void AccessModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.access_mode = DBConfig().options.access_mode;
}

Value AccessModeSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.access_mode) {
	case AccessMode::AUTOMATIC:
		return "automatic";
	case AccessMode::READ_ONLY:
		return "read_only";
	case AccessMode::READ_WRITE:
		return "read_write";
	default:
		throw InternalException("Unknown access mode setting");
	}
}

//===--------------------------------------------------------------------===//
// Checkpoint Threshold
//===--------------------------------------------------------------------===//
void CheckpointThresholdSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	idx_t new_limit = DBConfig::ParseMemoryLimit(input.ToString());
	config.options.checkpoint_wal_size = new_limit;
}

void CheckpointThresholdSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.checkpoint_wal_size = DBConfig().options.checkpoint_wal_size;
}

Value CheckpointThresholdSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.checkpoint_wal_size));
}

//===--------------------------------------------------------------------===//
// Debug Checkpoint Abort
//===--------------------------------------------------------------------===//
void DebugCheckpointAbort::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto checkpoint_abort = StringUtil::Lower(input.ToString());
	if (checkpoint_abort == "none") {
		config.options.checkpoint_abort = CheckpointAbort::NO_ABORT;
	} else if (checkpoint_abort == "before_truncate") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE;
	} else if (checkpoint_abort == "before_header") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER;
	} else if (checkpoint_abort == "after_free_list_write") {
		config.options.checkpoint_abort = CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE;
	} else {
		throw ParserException(
		    "Unrecognized option for PRAGMA debug_checkpoint_abort, expected none, before_truncate or before_header");
	}
}

void DebugCheckpointAbort::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.checkpoint_abort = DBConfig().options.checkpoint_abort;
}

Value DebugCheckpointAbort::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	auto setting = config.options.checkpoint_abort;
	switch (setting) {
	case CheckpointAbort::NO_ABORT:
		return "none";
	case CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE:
		return "before_truncate";
	case CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER:
		return "before_header";
	case CheckpointAbort::DEBUG_ABORT_AFTER_FREE_LIST_WRITE:
		return "after_free_list_write";
	default:
		throw InternalException("Type not implemented for CheckpointAbort");
	}
}

//===--------------------------------------------------------------------===//
// Debug Force External
//===--------------------------------------------------------------------===//
void DebugForceExternal::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_external = ClientConfig().force_external;
}

void DebugForceExternal::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_external = input.GetValue<bool>();
}

Value DebugForceExternal::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_external);
}

//===--------------------------------------------------------------------===//
// Debug Force NoCrossProduct
//===--------------------------------------------------------------------===//
void DebugForceNoCrossProduct::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_no_cross_product = ClientConfig().force_no_cross_product;
}

void DebugForceNoCrossProduct::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_no_cross_product = input.GetValue<bool>();
}

Value DebugForceNoCrossProduct::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_no_cross_product);
}

//===--------------------------------------------------------------------===//
// Ordered Aggregate Threshold
//===--------------------------------------------------------------------===//
void OrderedAggregateThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).ordered_aggregate_threshold = ClientConfig().ordered_aggregate_threshold;
}

void OrderedAggregateThreshold::SetLocal(ClientContext &context, const Value &input) {
	const auto param = input.GetValue<uint64_t>();
	if (param <= 0) {
		throw ParserException("Invalid option for PRAGMA ordered_aggregate_threshold, value must be positive");
	}
	ClientConfig::GetConfig(context).ordered_aggregate_threshold = param;
}

Value OrderedAggregateThreshold::GetSetting(ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).ordered_aggregate_threshold);
}

//===--------------------------------------------------------------------===//
// Debug Window Mode
//===--------------------------------------------------------------------===//
void DebugWindowMode::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto param = StringUtil::Lower(input.ToString());
	if (param == "window") {
		config.options.window_mode = WindowAggregationMode::WINDOW;
	} else if (param == "combine") {
		config.options.window_mode = WindowAggregationMode::COMBINE;
	} else if (param == "separate") {
		config.options.window_mode = WindowAggregationMode::SEPARATE;
	} else {
		throw ParserException("Unrecognized option for PRAGMA debug_window_mode, expected window, combine or separate");
	}
}

void DebugWindowMode::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.window_mode = DBConfig().options.window_mode;
}

Value DebugWindowMode::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Debug AsOf Join
//===--------------------------------------------------------------------===//
void DebugAsOfIEJoin::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).force_asof_iejoin = ClientConfig().force_asof_iejoin;
}

void DebugAsOfIEJoin::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).force_asof_iejoin = input.GetValue<bool>();
}

Value DebugAsOfIEJoin::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_asof_iejoin);
}

//===--------------------------------------------------------------------===//
// Prefer Range Joins
//===--------------------------------------------------------------------===//
void PreferRangeJoins::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).prefer_range_joins = ClientConfig().prefer_range_joins;
}

void PreferRangeJoins::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).prefer_range_joins = input.GetValue<bool>();
}

Value PreferRangeJoins::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).prefer_range_joins);
}

//===--------------------------------------------------------------------===//
// Default Collation
//===--------------------------------------------------------------------===//
void DefaultCollationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	config.options.collation = parameter;
}

void DefaultCollationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.collation = DBConfig().options.collation;
}

void DefaultCollationSetting::ResetLocal(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	config.options.collation = DBConfig().options.collation;
}

void DefaultCollationSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	// bind the collation to verify that it exists
	ExpressionBinder::TestCollation(context, parameter);
	auto &config = DBConfig::GetConfig(context);
	config.options.collation = parameter;
}

Value DefaultCollationSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.collation);
}

//===--------------------------------------------------------------------===//
// Default Order
//===--------------------------------------------------------------------===//
void DefaultOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "ascending" || parameter == "asc") {
		config.options.default_order_type = OrderType::ASCENDING;
	} else if (parameter == "descending" || parameter == "desc") {
		config.options.default_order_type = OrderType::DESCENDING;
	} else {
		throw InvalidInputException("Unrecognized parameter for option DEFAULT_ORDER \"%s\". Expected ASC or DESC.",
		                            parameter);
	}
}

void DefaultOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_order_type = DBConfig().options.default_order_type;
}

Value DefaultOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.default_order_type) {
	case OrderType::ASCENDING:
		return "asc";
	case OrderType::DESCENDING:
		return "desc";
	default:
		throw InternalException("Unknown order type setting");
	}
}

//===--------------------------------------------------------------------===//
// Default Null Order
//===--------------------------------------------------------------------===//
void DefaultNullOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());

	if (parameter == "nulls_first" || parameter == "nulls first" || parameter == "null first" || parameter == "first") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_FIRST;
	} else if (parameter == "nulls_last" || parameter == "nulls last" || parameter == "null last" ||
	           parameter == "last") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_LAST;
	} else if (parameter == "nulls_first_on_asc_last_on_desc" || parameter == "sqlite" || parameter == "mysql") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC;
	} else if (parameter == "nulls_last_on_asc_first_on_desc" || parameter == "postgres") {
		config.options.default_null_order = DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC;
	} else {
		throw ParserException("Unrecognized parameter for option NULL_ORDER \"%s\", expected either NULLS FIRST, NULLS "
		                      "LAST, SQLite, MySQL or Postgres",
		                      parameter);
	}
}

void DefaultNullOrderSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_null_order = DBConfig().options.default_null_order;
}

Value DefaultNullOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch (config.options.default_null_order) {
	case DefaultOrderByNullType::NULLS_FIRST:
		return "nulls_first";
	case DefaultOrderByNullType::NULLS_LAST:
		return "nulls_last";
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		return "nulls_first_on_asc_last_on_desc";
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		return "nulls_last_on_asc_first_on_desc";
	default:
		throw InternalException("Unknown null order setting");
	}
}

//===--------------------------------------------------------------------===//
// Disabled File Systems
//===--------------------------------------------------------------------===//
void DisabledFileSystemsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (!db) {
		throw InternalException("disabled_filesystems can only be set in an active database");
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	auto list = StringUtil::Split(input.ToString(), ",");
	fs.SetDisabledFileSystems(list);
}

void DisabledFileSystemsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (!db) {
		throw InternalException("disabled_filesystems can only be set in an active database");
	}
	auto &fs = FileSystem::GetFileSystem(*db);
	fs.SetDisabledFileSystems(vector<string>());
}

Value DisabledFileSystemsSetting::GetSetting(ClientContext &context) {
	return Value("");
}

//===--------------------------------------------------------------------===//
// Disabled Optimizer
//===--------------------------------------------------------------------===//
void DisabledOptimizersSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto list = StringUtil::Split(input.ToString(), ",");
	set<OptimizerType> disabled_optimizers;
	for (auto &entry : list) {
		auto param = StringUtil::Lower(entry);
		StringUtil::Trim(param);
		if (param.empty()) {
			continue;
		}
		disabled_optimizers.insert(OptimizerTypeFromString(param));
	}
	config.options.disabled_optimizers = std::move(disabled_optimizers);
}

void DisabledOptimizersSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.disabled_optimizers = DBConfig().options.disabled_optimizers;
}

Value DisabledOptimizersSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	string result;
	for (auto &optimizer : config.options.disabled_optimizers) {
		if (!result.empty()) {
			result += ",";
		}
		result += OptimizerTypeToString(optimizer);
	}
	return Value(result);
}

//===--------------------------------------------------------------------===//
// Enable External Access
//===--------------------------------------------------------------------===//
void EnableExternalAccessSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	if (db && new_value) {
		throw InvalidInputException("Cannot change enable_external_access setting while database is running");
	}
	config.options.enable_external_access = new_value;
}

void EnableExternalAccessSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change enable_external_access setting while database is running");
	}
	config.options.enable_external_access = DBConfig().options.enable_external_access;
}

Value EnableExternalAccessSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_external_access);
}

//===--------------------------------------------------------------------===//
// Enable FSST Vectors
//===--------------------------------------------------------------------===//
void EnableFSSTVectors::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_fsst_vectors = input.GetValue<bool>();
}

void EnableFSSTVectors::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_fsst_vectors = DBConfig().options.enable_fsst_vectors;
}

Value EnableFSSTVectors::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_fsst_vectors);
}

//===--------------------------------------------------------------------===//
// Allow Unsigned Extensions
//===--------------------------------------------------------------------===//
void AllowUnsignedExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	if (db && new_value) {
		throw InvalidInputException("Cannot change allow_unsigned_extensions setting while database is running");
	}
	config.options.allow_unsigned_extensions = new_value;
}

void AllowUnsignedExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change allow_unsigned_extensions setting while database is running");
	}
	config.options.allow_unsigned_extensions = DBConfig().options.allow_unsigned_extensions;
}

Value AllowUnsignedExtensionsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_unsigned_extensions);
}

//===--------------------------------------------------------------------===//
// Enable Object Cache
//===--------------------------------------------------------------------===//
void EnableObjectCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.object_cache_enable = input.GetValue<bool>();
}

void EnableObjectCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.object_cache_enable = DBConfig().options.object_cache_enable;
}

Value EnableObjectCacheSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.object_cache_enable);
}

//===--------------------------------------------------------------------===//
// Enable HTTP Metadata Cache
//===--------------------------------------------------------------------===//
void EnableHTTPMetadataCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.http_metadata_cache_enable = input.GetValue<bool>();
}

void EnableHTTPMetadataCacheSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.http_metadata_cache_enable = DBConfig().options.http_metadata_cache_enable;
}

Value EnableHTTPMetadataCacheSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.http_metadata_cache_enable);
}

//===--------------------------------------------------------------------===//
// Enable Profiling
//===--------------------------------------------------------------------===//
void EnableProfilingSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.profiler_print_format = ClientConfig().profiler_print_format;
	config.enable_profiler = ClientConfig().enable_profiler;
	config.emit_profiler_output = ClientConfig().emit_profiler_output;
}

void EnableProfilingSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());

	auto &config = ClientConfig::GetConfig(context);
	if (parameter == "json") {
		config.profiler_print_format = ProfilerPrintFormat::JSON;
	} else if (parameter == "query_tree") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else if (parameter == "query_tree_optimizer") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;
	} else {
		throw ParserException(
		    "Unrecognized print format %s, supported formats: [json, query_tree, query_tree_optimizer]", parameter);
	}
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

Value EnableProfilingSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_profiler) {
		return Value();
	}
	switch (config.profiler_print_format) {
	case ProfilerPrintFormat::JSON:
		return Value("json");
	case ProfilerPrintFormat::QUERY_TREE:
		return Value("query_tree");
	case ProfilerPrintFormat::QUERY_TREE_OPTIMIZER:
		return Value("query_tree_optimizer");
	default:
		throw InternalException("Unsupported profiler print format");
	}
}

//===--------------------------------------------------------------------===//
// Custom Extension Repository
//===--------------------------------------------------------------------===//
void CustomExtensionRepository::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).custom_extension_repo = ClientConfig().custom_extension_repo;
}

void CustomExtensionRepository::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).custom_extension_repo = StringUtil::Lower(input.ToString());
}

Value CustomExtensionRepository::GetSetting(ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).custom_extension_repo);
}

//===--------------------------------------------------------------------===//
// Autoload Extension Repository
//===--------------------------------------------------------------------===//
void AutoloadExtensionRepository::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).autoinstall_extension_repo = ClientConfig().autoinstall_extension_repo;
}

void AutoloadExtensionRepository::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).autoinstall_extension_repo = StringUtil::Lower(input.ToString());
}

Value AutoloadExtensionRepository::GetSetting(ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).autoinstall_extension_repo);
}

//===--------------------------------------------------------------------===//
// Autoinstall Known Extensions
//===--------------------------------------------------------------------===//
void AutoinstallKnownExtensions::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoinstall_known_extensions = input.GetValue<bool>();
}

void AutoinstallKnownExtensions::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoinstall_known_extensions = DBConfig().options.autoinstall_known_extensions;
}

Value AutoinstallKnownExtensions::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.autoinstall_known_extensions);
}
//===--------------------------------------------------------------------===//
// Autoload Known Extensions
//===--------------------------------------------------------------------===//
void AutoloadKnownExtensions::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoload_known_extensions = input.GetValue<bool>();
}

void AutoloadKnownExtensions::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoload_known_extensions = DBConfig().options.autoload_known_extensions;
}

Value AutoloadKnownExtensions::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.autoload_known_extensions);
}

//===--------------------------------------------------------------------===//
// Enable Progress Bar
//===--------------------------------------------------------------------===//
void EnableProgressBarSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.enable_progress_bar = ClientConfig().enable_progress_bar;
}

void EnableProgressBarSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.enable_progress_bar = input.GetValue<bool>();
}

Value EnableProgressBarSetting::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).enable_progress_bar);
}

//===--------------------------------------------------------------------===//
// Enable Progress Bar Print
//===--------------------------------------------------------------------===//
void EnableProgressBarPrintSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.print_progress_bar = input.GetValue<bool>();
}

void EnableProgressBarPrintSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.print_progress_bar = ClientConfig().print_progress_bar;
}

Value EnableProgressBarPrintSetting::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).print_progress_bar);
}

//===--------------------------------------------------------------------===//
// Explain Output
//===--------------------------------------------------------------------===//
void ExplainOutputSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).explain_output_type = ClientConfig().explain_output_type;
}

void ExplainOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "all") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::ALL;
	} else if (parameter == "optimized_only") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::OPTIMIZED_ONLY;
	} else if (parameter == "physical_only") {
		ClientConfig::GetConfig(context).explain_output_type = ExplainOutputType::PHYSICAL_ONLY;
	} else {
		throw ParserException("Unrecognized output type \"%s\", expected either ALL, OPTIMIZED_ONLY or PHYSICAL_ONLY",
		                      parameter);
	}
}

Value ExplainOutputSetting::GetSetting(ClientContext &context) {
	switch (ClientConfig::GetConfig(context).explain_output_type) {
	case ExplainOutputType::ALL:
		return "all";
	case ExplainOutputType::OPTIMIZED_ONLY:
		return "optimized_only";
	case ExplainOutputType::PHYSICAL_ONLY:
		return "physical_only";
	default:
		throw InternalException("Unrecognized explain output type");
	}
}

//===--------------------------------------------------------------------===//
// Extension Directory Setting
//===--------------------------------------------------------------------===//
void ExtensionDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_directory = input.ToString();
	config.options.extension_directory = input.ToString();
}

void ExtensionDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.extension_directory = DBConfig().options.extension_directory;
}

Value ExtensionDirectorySetting::GetSetting(ClientContext &context) {
	return Value(DBConfig::GetConfig(context).options.extension_directory);
}

//===--------------------------------------------------------------------===//
// External Threads Setting
//===--------------------------------------------------------------------===//
void ExternalThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.external_threads = input.GetValue<int64_t>();
}

void ExternalThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.external_threads = DBConfig().options.external_threads;
}

Value ExternalThreadsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(config.options.external_threads);
}

//===--------------------------------------------------------------------===//
// File Search Path
//===--------------------------------------------------------------------===//
void FileSearchPathSetting::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	client_data.file_search_path.clear();
}

void FileSearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.file_search_path = parameter;
}

Value FileSearchPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return Value(client_data.file_search_path);
}

//===--------------------------------------------------------------------===//
// Force Compression
//===--------------------------------------------------------------------===//
void ForceCompressionSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto compression = StringUtil::Lower(input.ToString());
	if (compression == "none" || compression == "auto") {
		config.options.force_compression = CompressionType::COMPRESSION_AUTO;
	} else {
		auto compression_type = CompressionTypeFromString(compression);
		if (compression_type == CompressionType::COMPRESSION_AUTO) {
			auto compression_types = StringUtil::Join(ListCompressionTypes(), ", ");
			throw ParserException("Unrecognized option for PRAGMA force_compression, expected %s", compression_types);
		}
		config.options.force_compression = compression_type;
	}
}

void ForceCompressionSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_compression = DBConfig().options.force_compression;
}

Value ForceCompressionSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return CompressionTypeToString(config.options.force_compression);
}

//===--------------------------------------------------------------------===//
// Force Bitpacking mode
//===--------------------------------------------------------------------===//
void ForceBitpackingModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto mode_str = StringUtil::Lower(input.ToString());
	auto mode = BitpackingModeFromString(mode_str);
	if (mode == BitpackingMode::INVALID) {
		throw ParserException("Unrecognized option for force_bitpacking_mode, expected none, constant, constant_delta, "
		                      "delta_for, or for");
	}
	config.options.force_bitpacking_mode = mode;
}

void ForceBitpackingModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.force_bitpacking_mode = DBConfig().options.force_bitpacking_mode;
}

Value ForceBitpackingModeSetting::GetSetting(ClientContext &context) {
	return Value(BitpackingModeToString(context.db->config.options.force_bitpacking_mode));
}

//===--------------------------------------------------------------------===//
// Home Directory
//===--------------------------------------------------------------------===//
void HomeDirectorySetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).home_directory = ClientConfig().home_directory;
}

void HomeDirectorySetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.home_directory = input.IsNull() ? string() : input.ToString();
}

Value HomeDirectorySetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.home_directory);
}

//===--------------------------------------------------------------------===//
// Integer Division
//===--------------------------------------------------------------------===//
void IntegerDivisionSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).integer_division = ClientConfig().integer_division;
}

void IntegerDivisionSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.integer_division = input.GetValue<bool>();
}

Value IntegerDivisionSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.integer_division);
}

//===--------------------------------------------------------------------===//
// Log Query Path
//===--------------------------------------------------------------------===//
void LogQueryPathSetting::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	// TODO: verify that this does the right thing
	client_data.log_query_writer = std::move(ClientData(context).log_query_writer);
}

void LogQueryPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &client_data = ClientData::Get(context);
	auto path = input.ToString();
	if (path.empty()) {
		// empty path: clean up query writer
		client_data.log_query_writer = nullptr;
	} else {
		client_data.log_query_writer = make_uniq<BufferedFileWriter>(FileSystem::GetFileSystem(context), path,
		                                                             BufferedFileWriter::DEFAULT_OPEN_FLAGS);
	}
}

Value LogQueryPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return client_data.log_query_writer ? Value(client_data.log_query_writer->path) : Value();
}

//===--------------------------------------------------------------------===//
// Lock Configuration
//===--------------------------------------------------------------------===//
void LockConfigurationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	config.options.lock_configuration = new_value;
}

void LockConfigurationSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.lock_configuration = DBConfig().options.lock_configuration;
}

Value LockConfigurationSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.lock_configuration);
}

//===--------------------------------------------------------------------===//
// Immediate Transaction Mode
//===--------------------------------------------------------------------===//
void ImmediateTransactionModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.immediate_transaction_mode = BooleanValue::Get(input);
}

void ImmediateTransactionModeSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.immediate_transaction_mode = DBConfig().options.immediate_transaction_mode;
}

Value ImmediateTransactionModeSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.immediate_transaction_mode);
}

//===--------------------------------------------------------------------===//
// Maximum Expression Depth
//===--------------------------------------------------------------------===//
void MaximumExpressionDepthSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).max_expression_depth = ClientConfig().max_expression_depth;
}

void MaximumExpressionDepthSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).max_expression_depth = input.GetValue<uint64_t>();
}

Value MaximumExpressionDepthSetting::GetSetting(ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).max_expression_depth);
}

//===--------------------------------------------------------------------===//
// Maximum Memory
//===--------------------------------------------------------------------===//
void MaximumMemorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.maximum_memory = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		BufferManager::GetBufferManager(*db).SetLimit(config.options.maximum_memory);
	}
}

void MaximumMemorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.SetDefaultMaxMemory();
}

Value MaximumMemorySetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.maximum_memory));
}

//===--------------------------------------------------------------------===//
// Password Setting
//===--------------------------------------------------------------------===//
void PasswordSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// nop
}

void PasswordSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// nop
}

Value PasswordSetting::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Perfect Hash Threshold
//===--------------------------------------------------------------------===//
void PerfectHashThresholdSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).perfect_ht_threshold = ClientConfig().perfect_ht_threshold;
}

void PerfectHashThresholdSetting::SetLocal(ClientContext &context, const Value &input) {
	auto bits = input.GetValue<int64_t>();
	if (bits < 0 || bits > 32) {
		throw ParserException("Perfect HT threshold out of range: should be within range 0 - 32");
	}
	ClientConfig::GetConfig(context).perfect_ht_threshold = bits;
}

Value PerfectHashThresholdSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).perfect_ht_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Filter Threshold
//===--------------------------------------------------------------------===//
void PivotFilterThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).pivot_filter_threshold = ClientConfig().pivot_filter_threshold;
}

void PivotFilterThreshold::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).pivot_filter_threshold = input.GetValue<uint64_t>();
}

Value PivotFilterThreshold::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).pivot_filter_threshold);
}

//===--------------------------------------------------------------------===//
// Pivot Limit
//===--------------------------------------------------------------------===//
void PivotLimitSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).pivot_limit = ClientConfig().pivot_limit;
}

void PivotLimitSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).pivot_limit = input.GetValue<uint64_t>();
}

Value PivotLimitSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).pivot_limit);
}

//===--------------------------------------------------------------------===//
// PreserveIdentifierCase
//===--------------------------------------------------------------------===//
void PreserveIdentifierCase::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).preserve_identifier_case = ClientConfig().preserve_identifier_case;
}

void PreserveIdentifierCase::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).preserve_identifier_case = input.GetValue<bool>();
}

Value PreserveIdentifierCase::GetSetting(ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).preserve_identifier_case);
}

//===--------------------------------------------------------------------===//
// PreserveInsertionOrder
//===--------------------------------------------------------------------===//
void PreserveInsertionOrder::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.preserve_insertion_order = input.GetValue<bool>();
}

void PreserveInsertionOrder::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.preserve_insertion_order = DBConfig().options.preserve_insertion_order;
}

Value PreserveInsertionOrder::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.preserve_insertion_order);
}

//===--------------------------------------------------------------------===//
// ExportLargeBufferArrow
//===--------------------------------------------------------------------===//
void ExportLargeBufferArrow::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto export_large_buffers_arrow = input.GetValue<bool>();

	config.options.arrow_offset_size = export_large_buffers_arrow ? ArrowOffsetSize::LARGE : ArrowOffsetSize::REGULAR;
}

void ExportLargeBufferArrow::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_offset_size = DBConfig().options.arrow_offset_size;
}

Value ExportLargeBufferArrow::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	bool export_large_buffers_arrow = config.options.arrow_offset_size == ArrowOffsetSize::LARGE;
	return Value::BOOLEAN(export_large_buffers_arrow);
}

//===--------------------------------------------------------------------===//
// Profiler History Size
//===--------------------------------------------------------------------===//
void ProfilerHistorySize::ResetLocal(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	client_data.query_profiler_history->ResetProfilerHistorySize();
}

void ProfilerHistorySize::SetLocal(ClientContext &context, const Value &input) {
	auto size = input.GetValue<int64_t>();
	if (size <= 0) {
		throw ParserException("Size should be >= 0");
	}
	auto &client_data = ClientData::Get(context);
	client_data.query_profiler_history->SetProfilerHistorySize(size);
}

Value ProfilerHistorySize::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Profile Output
//===--------------------------------------------------------------------===//
void ProfileOutputSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).profiler_save_location = ClientConfig().profiler_save_location;
}

void ProfileOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	auto parameter = input.ToString();
	config.profiler_save_location = parameter;
}

Value ProfileOutputSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.profiler_save_location);
}

//===--------------------------------------------------------------------===//
// Profiling Mode
//===--------------------------------------------------------------------===//
void ProfilingModeSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).enable_profiler = ClientConfig().enable_profiler;
	ClientConfig::GetConfig(context).enable_detailed_profiling = ClientConfig().enable_detailed_profiling;
	ClientConfig::GetConfig(context).emit_profiler_output = ClientConfig().emit_profiler_output;
}

void ProfilingModeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	auto &config = ClientConfig::GetConfig(context);
	if (parameter == "standard") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = false;
		config.emit_profiler_output = true;
	} else if (parameter == "detailed") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = true;
		config.emit_profiler_output = true;
	} else {
		throw ParserException("Unrecognized profiling mode \"%s\", supported formats: [standard, detailed]", parameter);
	}
}

Value ProfilingModeSetting::GetSetting(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	if (!config.enable_profiler) {
		return Value();
	}
	return Value(config.enable_detailed_profiling ? "detailed" : "standard");
}

//===--------------------------------------------------------------------===//
// Progress Bar Time
//===--------------------------------------------------------------------===//
void ProgressBarTimeSetting::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.wait_time = ClientConfig().wait_time;
	config.enable_progress_bar = ClientConfig().enable_progress_bar;
}

void ProgressBarTimeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	ProgressBar::SystemOverrideCheck(config);
	config.wait_time = input.GetValue<int32_t>();
	config.enable_progress_bar = true;
}

Value ProgressBarTimeSetting::GetSetting(ClientContext &context) {
	return Value::BIGINT(ClientConfig::GetConfig(context).wait_time);
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void SchemaSetting::ResetLocal(ClientContext &context) {
	// FIXME: catalog_search_path is controlled by both SchemaSetting and SearchPathSetting
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Reset();
}

void SchemaSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Set(CatalogSearchEntry::Parse(parameter), CatalogSetPathType::SET_SCHEMA);
}

Value SchemaSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return client_data.catalog_search_path->GetDefault().schema;
}

//===--------------------------------------------------------------------===//
// Search Path
//===--------------------------------------------------------------------===//
void SearchPathSetting::ResetLocal(ClientContext &context) {
	// FIXME: catalog_search_path is controlled by both SchemaSetting and SearchPathSetting
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Reset();
}

void SearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	auto &client_data = ClientData::Get(context);
	client_data.catalog_search_path->Set(CatalogSearchEntry::ParseList(parameter), CatalogSetPathType::SET_SCHEMAS);
}

Value SearchPathSetting::GetSetting(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	auto &set_paths = client_data.catalog_search_path->GetSetPaths();
	return Value(CatalogSearchEntry::ListToString(set_paths));
}

//===--------------------------------------------------------------------===//
// Temp Directory
//===--------------------------------------------------------------------===//
void TempDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.temporary_directory = input.ToString();
	config.options.use_temporary_directory = !config.options.temporary_directory.empty();
	if (db) {
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		buffer_manager.SetTemporaryDirectory(config.options.temporary_directory);
	}
}

void TempDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.temporary_directory = DBConfig().options.temporary_directory;
	config.options.use_temporary_directory = DBConfig().options.use_temporary_directory;
	if (db) {
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		buffer_manager.SetTemporaryDirectory(config.options.temporary_directory);
	}
}

Value TempDirectorySetting::GetSetting(ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	return Value(buffer_manager.GetTemporaryDirectory());
}

//===--------------------------------------------------------------------===//
// Threads Setting
//===--------------------------------------------------------------------===//
void ThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.maximum_threads = input.GetValue<int64_t>();
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(config.options.maximum_threads);
	}
}

void ThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.SetDefaultMaxThreads();
}

Value ThreadsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(config.options.maximum_threads);
}

//===--------------------------------------------------------------------===//
// Username Setting
//===--------------------------------------------------------------------===//
void UsernameSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	// nop
}

void UsernameSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	// nop
}

Value UsernameSetting::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Allocator Flush Threshold
//===--------------------------------------------------------------------===//
void FlushAllocatorSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_flush_threshold = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

void FlushAllocatorSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_flush_threshold = DBConfig().options.allocator_flush_threshold;
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

Value FlushAllocatorSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.allocator_flush_threshold));
}

} // namespace duckdb
