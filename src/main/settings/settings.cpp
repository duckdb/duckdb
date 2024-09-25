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
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

const string GetDefaultUserAgent() {
	return StringUtil::Format("duckdb/%s(%s)", DuckDB::LibraryVersion(), DuckDB::Platform());
}

//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void AccessModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (db) {
		throw InvalidInputException("Cannot change access_mode setting while database is running - it must be set when "
		                            "opening or attaching the database");
	}
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

Value AccessModeSetting::GetSetting(const ClientContext &context) {
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
// Allow Persistent Secrets
//===--------------------------------------------------------------------===//
void AllowPersistentSecrets::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto value = input.DefaultCastAs(LogicalType::BOOLEAN);
	config.secret_manager->SetEnablePersistentSecrets(value.GetValue<bool>());
}

void AllowPersistentSecrets::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.secret_manager->ResetEnablePersistentSecrets();
}

Value AllowPersistentSecrets::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.secret_manager->PersistentSecretsEnabled());
}

//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void CatalogErrorMaxSchema::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.catalog_error_max_schemas = UBigIntValue::Get(input);
}

void CatalogErrorMaxSchema::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.catalog_error_max_schemas = DBConfig().options.catalog_error_max_schemas;
}

Value CatalogErrorMaxSchema::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::UBIGINT(config.options.catalog_error_max_schemas);
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

Value CheckpointThresholdSetting::GetSetting(const ClientContext &context) {
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

Value DebugCheckpointAbort::GetSetting(const ClientContext &context) {
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

Value DebugForceExternal::GetSetting(const ClientContext &context) {
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

Value DebugForceNoCrossProduct::GetSetting(const ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).force_no_cross_product);
}

//===--------------------------------------------------------------------===//
// Debug Skip Checkpoint On Commit
//===--------------------------------------------------------------------===//
void DebugSkipCheckpointOnCommit::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
	config.options.debug_skip_checkpoint_on_commit = BooleanValue::Get(parameter);
}

void DebugSkipCheckpointOnCommit::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.debug_skip_checkpoint_on_commit = DBConfig().options.debug_skip_checkpoint_on_commit;
}

Value DebugSkipCheckpointOnCommit::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(*context.db);
	return Value::BOOLEAN(config.options.debug_skip_checkpoint_on_commit);
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

Value OrderedAggregateThreshold::GetSetting(const ClientContext &context) {
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

Value DebugWindowMode::GetSetting(const ClientContext &context) {
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

Value DebugAsOfIEJoin::GetSetting(const ClientContext &context) {
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

Value PreferRangeJoins::GetSetting(const ClientContext &context) {
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

Value DefaultCollationSetting::GetSetting(const ClientContext &context) {
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

Value DefaultOrderSetting::GetSetting(const ClientContext &context) {
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

Value DefaultNullOrderSetting::GetSetting(const ClientContext &context) {
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
// Default Secret Storage
//===--------------------------------------------------------------------===//
void DefaultSecretStorage::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.secret_manager->SetDefaultStorage(input.ToString());
}

void DefaultSecretStorage::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.secret_manager->ResetDefaultStorage();
}

Value DefaultSecretStorage::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return config.secret_manager->DefaultStorage();
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

Value DisabledFileSystemsSetting::GetSetting(const ClientContext &context) {
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

Value DisabledOptimizersSetting::GetSetting(const ClientContext &context) {
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

Value EnableExternalAccessSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_external_access);
}

//===--------------------------------------------------------------------===//
// Enable Macro Dependencies
//===--------------------------------------------------------------------===//
void EnableMacrosDependencies::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_macro_dependencies = input.GetValue<bool>();
}

void EnableMacrosDependencies::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_macro_dependencies = DBConfig().options.enable_macro_dependencies;
}

Value EnableMacrosDependencies::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_macro_dependencies);
}

//===--------------------------------------------------------------------===//
// Enable View Dependencies
//===--------------------------------------------------------------------===//
void EnableViewDependencies::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.enable_view_dependencies = input.GetValue<bool>();
}

void EnableViewDependencies::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.enable_view_dependencies = DBConfig().options.enable_view_dependencies;
}

Value EnableViewDependencies::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.enable_view_dependencies);
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

Value EnableFSSTVectors::GetSetting(const ClientContext &context) {
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

Value AllowUnsignedExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_unsigned_extensions);
}

//===--------------------------------------------------------------------===//
// Allow Community Extensions
//===--------------------------------------------------------------------===//
void AllowCommunityExtensionsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (db && !config.options.allow_community_extensions) {
		auto new_value = input.GetValue<bool>();
		if (new_value) {
			throw InvalidInputException("Cannot upgrade allow_community_extensions setting while database is running");
		}
		return;
	}
	auto new_value = input.GetValue<bool>();
	config.options.allow_community_extensions = new_value;
}

void AllowCommunityExtensionsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db && !config.options.allow_community_extensions) {
		if (DBConfig().options.allow_community_extensions) {
			throw InvalidInputException("Cannot upgrade allow_community_extensions setting while database is running");
		}
		return;
	}
	config.options.allow_community_extensions = DBConfig().options.allow_community_extensions;
}

Value AllowCommunityExtensionsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_community_extensions);
}

//===--------------------------------------------------------------------===//
// Allow Extensions Metadata Mismatch
//===--------------------------------------------------------------------===//
void AllowExtensionsMetadataMismatchSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	config.options.allow_extensions_metadata_mismatch = new_value;
}

void AllowExtensionsMetadataMismatchSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allow_extensions_metadata_mismatch = DBConfig().options.allow_extensions_metadata_mismatch;
}

Value AllowExtensionsMetadataMismatchSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_extensions_metadata_mismatch);
}

//===--------------------------------------------------------------------===//
// Allow Unredacted Secrets
//===--------------------------------------------------------------------===//
void AllowUnredactedSecretsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<bool>();
	if (db && new_value) {
		throw InvalidInputException("Cannot change allow_unredacted_secrets setting while database is running");
	}
	config.options.allow_unredacted_secrets = new_value;
}

void AllowUnredactedSecretsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change allow_unredacted_secrets setting while database is running");
	}
	config.options.allow_unredacted_secrets = DBConfig().options.allow_unredacted_secrets;
}

Value AllowUnredactedSecretsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.allow_unredacted_secrets);
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

Value EnableObjectCacheSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.object_cache_enable);
}

//===--------------------------------------------------------------------===//
// Storage Compatibility Version (for serialization)
//===--------------------------------------------------------------------===//
void StorageCompatibilityVersion::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto version_string = input.GetValue<string>();
	auto serialization_compatibility = SerializationCompatibility::FromString(version_string);
	config.options.serialization_compatibility = serialization_compatibility;
}

void StorageCompatibilityVersion::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.serialization_compatibility = DBConfig().options.serialization_compatibility;
}

Value StorageCompatibilityVersion::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);

	auto &version_name = config.options.serialization_compatibility.duckdb_version;
	return Value(version_name);
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

Value EnableHTTPMetadataCacheSetting::GetSetting(const ClientContext &context) {
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
	config.profiler_settings = ClientConfig().profiler_settings;
}

void EnableProfilingSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());

	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
	config.profiler_settings = ClientConfig().profiler_settings;

	if (parameter == "json") {
		config.profiler_print_format = ProfilerPrintFormat::JSON;
	} else if (parameter == "query_tree") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else if (parameter == "query_tree_optimizer") {
		config.profiler_print_format = ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;

		// add optimizer settings to the profiler settings
		auto optimizer_settings = MetricsUtils::GetOptimizerMetrics();
		for (auto &setting : optimizer_settings) {
			config.profiler_settings.insert(setting);
		}

		// add the phase timing settings to the profiler settings
		auto phase_timing_settings = MetricsUtils::GetPhaseTimingMetrics();
		for (auto &setting : phase_timing_settings) {
			config.profiler_settings.insert(setting);
		}
	} else if (parameter == "no_output") {
		config.profiler_print_format = ProfilerPrintFormat::NO_OUTPUT;
		config.emit_profiler_output = false;
	} else {
		throw ParserException(
		    "Unrecognized print format %s, supported formats: [json, query_tree, query_tree_optimizer, no_output]",
		    parameter);
	}
}

Value EnableProfilingSetting::GetSetting(const ClientContext &context) {
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
	case ProfilerPrintFormat::NO_OUTPUT:
		return Value("no_output");
	default:
		throw InternalException("Unsupported profiler print format");
	}
}

//===--------------------------------------------------------------------===//
// Custom Profiling Settings
//===--------------------------------------------------------------------===//

bool IsEnabledOptimizer(MetricsType metric, const set<OptimizerType> &disabled_optimizers) {
	auto matching_optimizer_type = MetricsUtils::GetOptimizerTypeByMetric(metric);
	if (matching_optimizer_type != OptimizerType::INVALID &&
	    disabled_optimizers.find(matching_optimizer_type) == disabled_optimizers.end()) {
		return true;
	}
	return false;
}

static profiler_settings_t FillTreeNodeSettings(unordered_map<string, string> &json,
                                                const set<OptimizerType> &disabled_optimizers) {
	profiler_settings_t metrics;

	string invalid_settings;
	for (auto &entry : json) {
		MetricsType setting;
		try {
			setting = EnumUtil::FromString<MetricsType>(StringUtil::Upper(entry.first));
		} catch (std::exception &ex) {
			if (!invalid_settings.empty()) {
				invalid_settings += ", ";
			}
			invalid_settings += entry.first;
			continue;
		}
		if (StringUtil::Lower(entry.second) == "true" &&
		    (!MetricsUtils::IsOptimizerMetric(setting) || IsEnabledOptimizer(setting, disabled_optimizers))) {
			metrics.insert(setting);
		}
	}

	if (!invalid_settings.empty()) {
		throw IOException("Invalid custom profiler settings: \"%s\"", invalid_settings);
	}
	return metrics;
}

void AddOptimizerMetrics(profiler_settings_t &settings, const set<OptimizerType> &disabled_optimizers) {
	if (settings.find(MetricsType::ALL_OPTIMIZERS) != settings.end()) {
		auto optimizer_metrics = MetricsUtils::GetOptimizerMetrics();
		for (auto &metric : optimizer_metrics) {
			if (IsEnabledOptimizer(metric, disabled_optimizers)) {
				settings.insert(metric);
			}
		}
	}
}

void CustomProfilingSettings::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);

	// parse the file content
	unordered_map<string, string> json;
	try {
		json = StringUtil::ParseJSONMap(input.ToString());
	} catch (std::exception &ex) {
		throw IOException("Could not parse the custom profiler settings file due to incorrect JSON: \"%s\".  Make sure "
		                  "all the keys and values start with a quote. ",
		                  input.ToString());
	}

	config.enable_profiler = true;
	auto &db_config = DBConfig::GetConfig(context);
	auto &disabled_optimizers = db_config.options.disabled_optimizers;

	auto settings = FillTreeNodeSettings(json, disabled_optimizers);
	AddOptimizerMetrics(settings, disabled_optimizers);
	config.profiler_settings = settings;
}

void CustomProfilingSettings::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = ClientConfig().enable_profiler;
	config.profiler_settings = ProfilingInfo::DefaultSettings();
}

Value CustomProfilingSettings::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);

	string profiling_settings_str;
	for (auto &entry : config.profiler_settings) {
		if (!profiling_settings_str.empty()) {
			profiling_settings_str += ", ";
		}
		profiling_settings_str += StringUtil::Format("\"%s\": \"true\"", EnumUtil::ToString(entry));
	}
	return Value(StringUtil::Format("{%s}", profiling_settings_str));
}

//===--------------------------------------------------------------------===//
// Custom Extension Repository
//===--------------------------------------------------------------------===//
void CustomExtensionRepository::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.custom_extension_repo = DBConfig().options.custom_extension_repo;
}

void CustomExtensionRepository::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.custom_extension_repo = input.ToString();
}

Value CustomExtensionRepository::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.custom_extension_repo);
}

//===--------------------------------------------------------------------===//
// Autoload Extension Repository
//===--------------------------------------------------------------------===//
void AutoloadExtensionRepository::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.autoinstall_extension_repo = DBConfig().options.autoinstall_extension_repo;
}

void AutoloadExtensionRepository::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.autoinstall_extension_repo = input.ToString();
}

Value AutoloadExtensionRepository::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.autoinstall_extension_repo);
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

Value AutoinstallKnownExtensions::GetSetting(const ClientContext &context) {
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

Value AutoloadKnownExtensions::GetSetting(const ClientContext &context) {
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

Value EnableProgressBarSetting::GetSetting(const ClientContext &context) {
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

Value EnableProgressBarPrintSetting::GetSetting(const ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).print_progress_bar);
}

//===--------------------------------------------------------------------===//
// Errors As JSON
//===--------------------------------------------------------------------===//
void ErrorsAsJsonSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).errors_as_json = ClientConfig().errors_as_json;
}

void ErrorsAsJsonSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).errors_as_json = BooleanValue::Get(input);
}

Value ErrorsAsJsonSetting::GetSetting(const ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).errors_as_json);
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

Value ExplainOutputSetting::GetSetting(const ClientContext &context) {
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
	config.options.extension_directory = input.ToString();
}

void ExtensionDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.extension_directory = DBConfig().options.extension_directory;
}

Value ExtensionDirectorySetting::GetSetting(const ClientContext &context) {
	return Value(DBConfig::GetConfig(context).options.extension_directory);
}

//===--------------------------------------------------------------------===//
// External Threads Setting
//===--------------------------------------------------------------------===//
void ExternalThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_val = input.GetValue<int64_t>();
	if (new_val < 0) {
		throw SyntaxException("Must have a non-negative number of external threads!");
	}
	auto new_external_threads = NumericCast<idx_t>(new_val);
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(config.options.maximum_threads, new_external_threads);
	}
	config.options.external_threads = new_external_threads;
}

void ExternalThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	idx_t new_external_threads = DBConfig().options.external_threads;
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(config.options.maximum_threads, new_external_threads);
	}
	config.options.external_threads = new_external_threads;
}

Value ExternalThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(NumericCast<int64_t>(config.options.external_threads));
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

Value FileSearchPathSetting::GetSetting(const ClientContext &context) {
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
		if (CompressionTypeIsDeprecated(compression_type)) {
			throw ParserException("Attempted to force a deprecated compression type (%s)",
			                      CompressionTypeToString(compression_type));
		}
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

Value ForceCompressionSetting::GetSetting(const ClientContext &context) {
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

Value ForceBitpackingModeSetting::GetSetting(const ClientContext &context) {
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

	if (!input.IsNull() && FileSystem::GetFileSystem(context).IsRemoteFile(input.ToString())) {
		throw InvalidInputException("Cannot set the home directory to a remote path");
	}

	config.home_directory = input.IsNull() ? string() : input.ToString();
}

Value HomeDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(config.home_directory);
}

//===--------------------------------------------------------------------===//
// HTTP Proxy
//===--------------------------------------------------------------------===//
void HTTPProxy::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.http_proxy = DBConfig().options.http_proxy;
}

void HTTPProxy::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
	config.options.http_proxy = parameter.GetValue<string>();
}

Value HTTPProxy::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return config.options.http_proxy;
}

//===--------------------------------------------------------------------===//
// HTTP Proxy Username
//===--------------------------------------------------------------------===//
void HTTPProxyUsername::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.http_proxy_username = DBConfig().options.http_proxy_username;
}

void HTTPProxyUsername::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
	config.options.http_proxy_username = parameter.GetValue<string>();
}

Value HTTPProxyUsername::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return config.options.http_proxy_username;
}

//===--------------------------------------------------------------------===//
// HTTP Proxy Password
//===--------------------------------------------------------------------===//
void HTTPProxyPassword::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.http_proxy_password = DBConfig().options.http_proxy_password;
}

void HTTPProxyPassword::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &parameter) {
	config.options.http_proxy_password = parameter.GetValue<string>();
}

Value HTTPProxyPassword::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return config.options.http_proxy_password;
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

Value IntegerDivisionSetting::GetSetting(const ClientContext &context) {
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

Value LogQueryPathSetting::GetSetting(const ClientContext &context) {
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

Value LockConfigurationSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.lock_configuration);
}

//===--------------------------------------------------------------------===//
// IEEE Floating Points
//===--------------------------------------------------------------------===//
void IEEEFloatingPointOpsSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).ieee_floating_point_ops = ClientConfig().ieee_floating_point_ops;
}

void IEEEFloatingPointOpsSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).ieee_floating_point_ops = input.GetValue<bool>();
}

Value IEEEFloatingPointOpsSetting::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value::BOOLEAN(config.ieee_floating_point_ops);
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

Value ImmediateTransactionModeSetting::GetSetting(const ClientContext &context) {
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

Value MaximumExpressionDepthSetting::GetSetting(const ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).max_expression_depth);
}

//===--------------------------------------------------------------------===//
// Maximum Memory
//===--------------------------------------------------------------------===//
void MaximumMemorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.maximum_memory = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		BufferManager::GetBufferManager(*db).SetMemoryLimit(config.options.maximum_memory);
	}
}

void MaximumMemorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.SetDefaultMaxMemory();
}

Value MaximumMemorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.maximum_memory));
}

//===--------------------------------------------------------------------===//
// Streaming Buffer Size
//===--------------------------------------------------------------------===//
void StreamingBufferSize::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.streaming_buffer_size = DBConfig::ParseMemoryLimit(input.ToString());
}

void StreamingBufferSize::ResetLocal(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	config.SetDefaultStreamingBufferSize();
}

Value StreamingBufferSize::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.streaming_buffer_size));
}

//===--------------------------------------------------------------------===//
// Maximum Temp Directory Size
//===--------------------------------------------------------------------===//
void MaximumTempDirectorySize::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto maximum_swap_space = DBConfig::ParseMemoryLimit(input.ToString());
	if (maximum_swap_space == DConstants::INVALID_INDEX) {
		// We use INVALID_INDEX to indicate that the value is not set by the user
		// use one lower to indicate 'unlimited'
		maximum_swap_space--;
	}
	if (!db) {
		config.options.maximum_swap_space = maximum_swap_space;
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(*db);
	buffer_manager.SetSwapLimit(maximum_swap_space);
	config.options.maximum_swap_space = maximum_swap_space;
}

void MaximumTempDirectorySize::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.maximum_swap_space = DConstants::INVALID_INDEX;
	if (!db) {
		return;
	}
	auto &buffer_manager = BufferManager::GetBufferManager(*db);
	buffer_manager.SetSwapLimit();
}

Value MaximumTempDirectorySize::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	if (config.options.maximum_swap_space != DConstants::INVALID_INDEX) {
		// Explicitly set by the user
		return Value(StringUtil::BytesToHumanReadableString(config.options.maximum_swap_space));
	}
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	// Database is initialized, use the setting from the temporary directory
	auto max_swap = buffer_manager.GetMaxSwap();
	if (max_swap.IsValid()) {
		return Value(StringUtil::BytesToHumanReadableString(max_swap.GetIndex()));
	} else {
		// The temp directory has not been used yet
		return Value(StringUtil::BytesToHumanReadableString(0));
	}
}

//===--------------------------------------------------------------------===//
// Maximum Vacuum Size
//===--------------------------------------------------------------------===//
void MaximumVacuumTasks::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.max_vacuum_tasks = input.GetValue<uint64_t>();
}

void MaximumVacuumTasks::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.max_vacuum_tasks = DBConfig().options.max_vacuum_tasks;
}

Value MaximumVacuumTasks::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::UBIGINT(config.options.max_vacuum_tasks);
}

//===--------------------------------------------------------------------===//
// Merge Join Threshold
//===--------------------------------------------------------------------===//
void MergeJoinThreshold::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.merge_join_threshold = input.GetValue<idx_t>();
}

void MergeJoinThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).merge_join_threshold = ClientConfig().merge_join_threshold;
}

Value MergeJoinThreshold::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value::UBIGINT(config.merge_join_threshold);
}

//===--------------------------------------------------------------------===//
// Nested Loop Join Threshold
//===--------------------------------------------------------------------===//
void NestedLoopJoinThreshold::SetLocal(ClientContext &context, const Value &input) {
	auto &config = ClientConfig::GetConfig(context);
	config.nested_loop_join_threshold = input.GetValue<idx_t>();
}

void NestedLoopJoinThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).nested_loop_join_threshold = ClientConfig().nested_loop_join_threshold;
}

Value NestedLoopJoinThreshold::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value::UBIGINT(config.nested_loop_join_threshold);
}

//===--------------------------------------------------------------------===//
// Old Implicit Casting
//===--------------------------------------------------------------------===//
void OldImplicitCasting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.old_implicit_casting = input.GetValue<bool>();
}

void OldImplicitCasting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.old_implicit_casting = DBConfig().options.old_implicit_casting;
}

Value OldImplicitCasting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.options.old_implicit_casting);
}

//===--------------------------------------------------------------------===//
// Old Implicit Casting
//===--------------------------------------------------------------------===//
void OrderByNonIntegerLiteral::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).order_by_non_integer_literal = ClientConfig().order_by_non_integer_literal;
}

void OrderByNonIntegerLiteral::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).order_by_non_integer_literal = input.GetValue<bool>();
}

Value OrderByNonIntegerLiteral::GetSetting(const ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	return Value::BOOLEAN(config.order_by_non_integer_literal);
}

//===--------------------------------------------------------------------===//
// Partitioned Write Flush Threshold
//===--------------------------------------------------------------------===//
void PartitionedWriteFlushThreshold::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).partitioned_write_flush_threshold =
	    ClientConfig().partitioned_write_flush_threshold;
}

void PartitionedWriteFlushThreshold::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).partitioned_write_flush_threshold = input.GetValue<idx_t>();
}

Value PartitionedWriteFlushThreshold::GetSetting(const ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).partitioned_write_flush_threshold);
}

//===--------------------------------------------------------------------===//
// Partitioned Write Flush Threshold
//===--------------------------------------------------------------------===//
void PartitionedWriteMaxOpenFiles::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).partitioned_write_max_open_files = ClientConfig().partitioned_write_max_open_files;
}

void PartitionedWriteMaxOpenFiles::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).partitioned_write_max_open_files = input.GetValue<idx_t>();
}

Value PartitionedWriteMaxOpenFiles::GetSetting(const ClientContext &context) {
	return Value::UBIGINT(ClientConfig::GetConfig(context).partitioned_write_max_open_files);
}

//===--------------------------------------------------------------------===//
// Preferred block allocation size
//===--------------------------------------------------------------------===//
void DefaultBlockAllocSize::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto block_alloc_size = input.GetValue<uint64_t>();
	Storage::VerifyBlockAllocSize(block_alloc_size);
	config.options.default_block_alloc_size = block_alloc_size;
}

void DefaultBlockAllocSize::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.default_block_alloc_size = DBConfig().options.default_block_alloc_size;
}

Value DefaultBlockAllocSize::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::UBIGINT(config.options.default_block_alloc_size);
}

//===--------------------------------------------------------------------===//
// Index scan percentage
//===--------------------------------------------------------------------===//
void IndexScanPercentage::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto index_scan_percentage = input.GetValue<double>();
	if (index_scan_percentage < 0 || index_scan_percentage > 1.0) {
		throw InvalidInputException("the index scan percentage must be within [0, 1]");
	}
	config.options.index_scan_percentage = index_scan_percentage;
}

void IndexScanPercentage::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.index_scan_percentage = DBConfig().options.index_scan_percentage;
}

Value IndexScanPercentage::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::DOUBLE(config.options.index_scan_percentage);
}

//===--------------------------------------------------------------------===//
// Index scan max count
//===--------------------------------------------------------------------===//
void IndexScanMaxCount::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto index_scan_max_count = input.GetValue<uint64_t>();
	config.options.index_scan_max_count = index_scan_max_count;
}

void IndexScanMaxCount::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.index_scan_max_count = DBConfig().options.index_scan_max_count;
}

Value IndexScanMaxCount::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::UBIGINT(config.options.index_scan_max_count);
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

Value PasswordSetting::GetSetting(const ClientContext &context) {
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
	ClientConfig::GetConfig(context).perfect_ht_threshold = NumericCast<idx_t>(bits);
}

Value PerfectHashThresholdSetting::GetSetting(const ClientContext &context) {
	return Value::BIGINT(NumericCast<int64_t>(ClientConfig::GetConfig(context).perfect_ht_threshold));
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

Value PivotFilterThreshold::GetSetting(const ClientContext &context) {
	return Value::BIGINT(NumericCast<int64_t>(ClientConfig::GetConfig(context).pivot_filter_threshold));
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

Value PivotLimitSetting::GetSetting(const ClientContext &context) {
	return Value::BIGINT(NumericCast<int64_t>(ClientConfig::GetConfig(context).pivot_limit));
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

Value PreserveIdentifierCase::GetSetting(const ClientContext &context) {
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

Value PreserveInsertionOrder::GetSetting(const ClientContext &context) {
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

Value ExportLargeBufferArrow::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	bool export_large_buffers_arrow = config.options.arrow_offset_size == ArrowOffsetSize::LARGE;
	return Value::BOOLEAN(export_large_buffers_arrow);
}

//===--------------------------------------------------------------------===//
// ArrowOutputListView
//===--------------------------------------------------------------------===//
void ArrowOutputListView::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto arrow_output_list_view = input.GetValue<bool>();

	config.options.arrow_use_list_view = arrow_output_list_view;
}

void ArrowOutputListView::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_use_list_view = DBConfig().options.arrow_use_list_view;
}

Value ArrowOutputListView::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	bool arrow_output_list_view = config.options.arrow_use_list_view;
	return Value::BOOLEAN(arrow_output_list_view);
}

//===--------------------------------------------------------------------===//
// LosslessConversionArrow
//===--------------------------------------------------------------------===//
void LosslessConversionArrow::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto arrow_arrow_lossless_conversion = input.GetValue<bool>();

	config.options.arrow_arrow_lossless_conversion = arrow_arrow_lossless_conversion;
}

void LosslessConversionArrow::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.arrow_arrow_lossless_conversion = DBConfig().options.arrow_arrow_lossless_conversion;
}

Value LosslessConversionArrow::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	bool arrow_arrow_lossless_conversion = config.options.arrow_arrow_lossless_conversion;
	return Value::BOOLEAN(arrow_arrow_lossless_conversion);
}

//===--------------------------------------------------------------------===//
// ProduceArrowStringView
//===--------------------------------------------------------------------===//
void ProduceArrowStringView::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.produce_arrow_string_views = input.GetValue<bool>();
}

void ProduceArrowStringView::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.produce_arrow_string_views = DBConfig().options.produce_arrow_string_views;
}

Value ProduceArrowStringView::GetSetting(const ClientContext &context) {
	return Value::BOOLEAN(DBConfig::GetConfig(context).options.produce_arrow_string_views);
}

//===--------------------------------------------------------------------===//
// ScalarSubqueryErrorOnMultipleRows
//===--------------------------------------------------------------------===//
void ScalarSubqueryErrorOnMultipleRows::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).scalar_subquery_error_on_multiple_rows =
	    ClientConfig().scalar_subquery_error_on_multiple_rows;
}

void ScalarSubqueryErrorOnMultipleRows::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).scalar_subquery_error_on_multiple_rows = input.GetValue<bool>();
}

Value ScalarSubqueryErrorOnMultipleRows::GetSetting(const ClientContext &context) {
	return Value::BOOLEAN(ClientConfig::GetConfig(context).scalar_subquery_error_on_multiple_rows);
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

Value ProfileOutputSetting::GetSetting(const ClientContext &context) {
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
	ClientConfig::GetConfig(context).profiler_settings = ClientConfig().profiler_settings;
}

void ProfilingModeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	auto &config = ClientConfig::GetConfig(context);
	if (parameter == "standard") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = false;
	} else if (parameter == "detailed") {
		config.enable_profiler = true;
		config.enable_detailed_profiling = true;

		// add optimizer settings to the profiler settings
		auto optimizer_settings = MetricsUtils::GetOptimizerMetrics();
		for (auto &setting : optimizer_settings) {
			config.profiler_settings.insert(setting);
		}

		// add the phase timing settings to the profiler settings
		auto phase_timing_settings = MetricsUtils::GetPhaseTimingMetrics();
		for (auto &setting : phase_timing_settings) {
			config.profiler_settings.insert(setting);
		}
	} else {
		throw ParserException("Unrecognized profiling mode \"%s\", supported formats: [standard, detailed]", parameter);
	}
}

Value ProfilingModeSetting::GetSetting(const ClientContext &context) {
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

Value ProgressBarTimeSetting::GetSetting(const ClientContext &context) {
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

Value SchemaSetting::GetSetting(const ClientContext &context) {
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

Value SearchPathSetting::GetSetting(const ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	auto &set_paths = client_data.catalog_search_path->GetSetPaths();
	return Value(CatalogSearchEntry::ListToString(set_paths));
}

//===--------------------------------------------------------------------===//
// Secret Directory
//===--------------------------------------------------------------------===//
void SecretDirectorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.secret_manager->SetPersistentSecretPath(input.ToString());
}

void SecretDirectorySetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.secret_manager->ResetPersistentSecretPath();
}

Value SecretDirectorySetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return config.secret_manager->PersistentSecretPath();
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
	config.SetDefaultTempDirectory();

	config.options.use_temporary_directory = DBConfig().options.use_temporary_directory;
	if (db) {
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		buffer_manager.SetTemporaryDirectory(config.options.temporary_directory);
	}
}

Value TempDirectorySetting::GetSetting(const ClientContext &context) {
	auto &buffer_manager = BufferManager::GetBufferManager(context);
	return Value(buffer_manager.GetTemporaryDirectory());
}

//===--------------------------------------------------------------------===//
// Threads Setting
//===--------------------------------------------------------------------===//
void ThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_val = input.GetValue<int64_t>();
	if (new_val < 1) {
		throw SyntaxException("Must have at least 1 thread!");
	}
	auto new_maximum_threads = NumericCast<idx_t>(new_val);
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(new_maximum_threads, config.options.external_threads);
	}
	config.options.maximum_threads = new_maximum_threads;
}

void ThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	idx_t new_maximum_threads = config.GetSystemMaxThreads(*config.file_system);
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(new_maximum_threads, config.options.external_threads);
	}
	config.options.maximum_threads = new_maximum_threads;
}

Value ThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(NumericCast<int64_t>(config.options.maximum_threads));
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

Value UsernameSetting::GetSetting(const ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Allocator Flush Threshold
//===--------------------------------------------------------------------===//
void AllocatorFlushThreshold::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_flush_threshold = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

void AllocatorFlushThreshold::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_flush_threshold = DBConfig().options.allocator_flush_threshold;
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorFlushTreshold(config.options.allocator_flush_threshold);
	}
}

Value AllocatorFlushThreshold::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.allocator_flush_threshold));
}

//===--------------------------------------------------------------------===//
// Allocator Bulk Deallocation Flush Threshold
//===--------------------------------------------------------------------===//
void AllocatorBulkDeallocationFlushThreshold::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_bulk_deallocation_flush_threshold = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		BufferManager::GetBufferManager(*db).GetBufferPool().SetAllocatorBulkDeallocationFlushThreshold(
		    config.options.allocator_bulk_deallocation_flush_threshold);
	}
}

void AllocatorBulkDeallocationFlushThreshold::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_bulk_deallocation_flush_threshold =
	    DBConfig().options.allocator_bulk_deallocation_flush_threshold;
	if (db) {
		BufferManager::GetBufferManager(*db).GetBufferPool().SetAllocatorBulkDeallocationFlushThreshold(
		    config.options.allocator_bulk_deallocation_flush_threshold);
	}
}

Value AllocatorBulkDeallocationFlushThreshold::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.options.allocator_bulk_deallocation_flush_threshold));
}

//===--------------------------------------------------------------------===//
// Allocator Background Threads
//===--------------------------------------------------------------------===//
void AllocatorBackgroundThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.options.allocator_background_threads = input.GetValue<bool>();
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorBackgroundThreads(config.options.allocator_background_threads);
	}
}

void AllocatorBackgroundThreadsSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	config.options.allocator_background_threads = DBConfig().options.allocator_background_threads;
	if (db) {
		TaskScheduler::GetScheduler(*db).SetAllocatorBackgroundThreads(config.options.allocator_background_threads);
	}
}

Value AllocatorBackgroundThreadsSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.allocator_background_threads);
}

//===--------------------------------------------------------------------===//
// DuckDBApi Setting
//===--------------------------------------------------------------------===//

void DuckDBApiSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<string>();
	if (db) {
		throw InvalidInputException("Cannot change duckdb_api setting while database is running");
	}
	config.options.duckdb_api = new_value;
}

void DuckDBApiSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change duckdb_api setting while database is running");
	}
	config.options.duckdb_api = GetDefaultUserAgent();
}

Value DuckDBApiSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.duckdb_api);
}

//===--------------------------------------------------------------------===//
// CustomUserAgent Setting
//===--------------------------------------------------------------------===//

void CustomUserAgentSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto new_value = input.GetValue<string>();
	if (db) {
		throw InvalidInputException("Cannot change custom_user_agent setting while database is running");
	}
	config.options.custom_user_agent =
	    config.options.custom_user_agent.empty() ? new_value : config.options.custom_user_agent + " " + new_value;
}

void CustomUserAgentSetting::ResetGlobal(DatabaseInstance *db, DBConfig &config) {
	if (db) {
		throw InvalidInputException("Cannot change custom_user_agent setting while database is running");
	}
	config.options.custom_user_agent = DBConfig().options.custom_user_agent;
}

Value CustomUserAgentSetting::GetSetting(const ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.options.custom_user_agent);
}

//===--------------------------------------------------------------------===//
// EnableHTTPLogging Setting
//===--------------------------------------------------------------------===//
void EnableHTTPLoggingSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).enable_http_logging = ClientConfig().enable_http_logging;
}

void EnableHTTPLoggingSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).enable_http_logging = input.GetValue<bool>();
}

Value EnableHTTPLoggingSetting::GetSetting(const ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).enable_http_logging);
}

//===--------------------------------------------------------------------===//
// HTTPLoggingOutput Setting
//===--------------------------------------------------------------------===//
void HTTPLoggingOutputSetting::ResetLocal(ClientContext &context) {
	ClientConfig::GetConfig(context).http_logging_output = ClientConfig().http_logging_output;
}

void HTTPLoggingOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	ClientConfig::GetConfig(context).http_logging_output = input.GetValue<string>();
}

Value HTTPLoggingOutputSetting::GetSetting(const ClientContext &context) {
	return Value(ClientConfig::GetConfig(context).http_logging_output);
}

} // namespace duckdb
