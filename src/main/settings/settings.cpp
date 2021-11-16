#include "duckdb/main/settings.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Access Mode
//===--------------------------------------------------------------------===//
void AccessModeSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "automatic") {
		config.access_mode = AccessMode::AUTOMATIC;
	} else if (parameter == "read_only") {
		config.access_mode = AccessMode::READ_ONLY;
	} else if (parameter == "read_write") {
		config.access_mode = AccessMode::READ_WRITE;
	} else {
		throw InvalidInputException(
			"Unrecognized parameter for option ACCESS_MODE \"%s\". Expected READ_ONLY or READ_WRITE.", parameter);
	}
}

Value AccessModeSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch(config.access_mode) {
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
// Default Collation
//===--------------------------------------------------------------------===//
void DefaultCollationSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	config.collation = parameter;
}

void DefaultCollationSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	// bind the collation to verify that it exists
	ExpressionBinder::TestCollation(context, parameter);
	auto &config = DBConfig::GetConfig(context);
	config.collation = parameter;
}

Value DefaultCollationSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(config.collation);
}

//===--------------------------------------------------------------------===//
// Default Order
//===--------------------------------------------------------------------===//
void DefaultOrderSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "ascending" || parameter == "asc") {
		config.default_order_type = OrderType::ASCENDING;
	} else if (parameter == "descending" || parameter == "desc") {
		config.default_order_type = OrderType::DESCENDING;
	} else {
		throw InvalidInputException("Unrecognized parameter for option DEFAULT_ORDER \"%s\". Expected ASC or DESC.",
									parameter);
	}
}

Value DefaultOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch(config.default_order_type) {
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
		config.default_null_order = OrderByNullType::NULLS_FIRST;
	} else if (parameter == "nulls_last" || parameter == "nulls last" || parameter == "null last" || parameter == "last") {
		config.default_null_order = OrderByNullType::NULLS_LAST;
	} else {
		throw ParserException("Unrecognized parameter for option NULL_ORDER \"%s\", expected either NULLS FIRST or NULLS LAST",
		                      parameter);
	}
}

Value DefaultNullOrderSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	switch(config.default_null_order) {
	case OrderByNullType::NULLS_FIRST:
		return "nulls_first";
	case OrderByNullType::NULLS_LAST:
		return "nulls_last";
	default:
		throw InternalException("Unknown null order setting");
	}
}

//===--------------------------------------------------------------------===//
// Enable External Access
//===--------------------------------------------------------------------===//
void EnableExternalAccessSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	if (db) {
		throw InvalidInputException("Cannot change enable_external_access setting while database is running");
	}
	config.enable_external_access = input.GetValue<bool>();
}

Value EnableExternalAccessSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.enable_external_access);
}

//===--------------------------------------------------------------------===//
// Enable Object Cache
//===--------------------------------------------------------------------===//
void EnableObjectCacheSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.object_cache_enable = input.GetValue<bool>();
}

Value EnableObjectCacheSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BOOLEAN(config.object_cache_enable);
}

//===--------------------------------------------------------------------===//
// Enable Profiling
//===--------------------------------------------------------------------===//
void EnableProfilingSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "json") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::JSON;
	} else if (parameter == "query_tree") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else if (parameter == "query_tree_optimizer") {
		context.profiler->automatic_print_format = ProfilerPrintFormat::QUERY_TREE_OPTIMIZER;
	} else {
		throw ParserException(
		    "Unrecognized print format %s, supported formats: [json, query_tree, query_tree_optimizer]", parameter);
	}
	context.profiler->Enable();
}

Value EnableProfilingSetting::GetSetting(ClientContext &context) {
	if (!context.profiler->IsEnabled()) {
		return Value();
	}
	switch(context.profiler->automatic_print_format) {
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
// Explain Output
//===--------------------------------------------------------------------===//
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
	switch(ClientConfig::GetConfig(context).explain_output_type) {
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
// Log Query Path
//===--------------------------------------------------------------------===//
void LogQueryPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto path = input.ToString();
	if (path.empty()) {
		// empty path: clean up query writer
		context.log_query_writer = nullptr;
	} else {
		context.log_query_writer =
		    make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(context), path,
		                                    BufferedFileWriter::DEFAULT_OPEN_FLAGS, context.file_opener.get());
	}
}

Value LogQueryPathSetting::GetSetting(ClientContext &context) {
	return context.log_query_writer ? Value(context.log_query_writer->path) : Value();
}

//===--------------------------------------------------------------------===//
// Maximum Memory
//===--------------------------------------------------------------------===//
void MaximumMemorySetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.maximum_memory = DBConfig::ParseMemoryLimit(input.ToString());
	if (db) {
		BufferManager::GetBufferManager(*db).SetLimit(config.maximum_memory);
	}
}

Value MaximumMemorySetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value(StringUtil::BytesToHumanReadableString(config.maximum_memory));
}

//===--------------------------------------------------------------------===//
// Profile Output
//===--------------------------------------------------------------------===//
void ProfileOutputSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	context.profiler->save_location = parameter;
}

Value ProfileOutputSetting::GetSetting(ClientContext &context) {
	return Value(context.profiler->save_location);
}

//===--------------------------------------------------------------------===//
// Profiling Mode
//===--------------------------------------------------------------------===//
void ProfilingModeSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = StringUtil::Lower(input.ToString());
	if (parameter == "standard") {
		context.profiler->Enable();
	} else if (parameter == "detailed") {
		context.profiler->DetailedEnable();
	} else {
		throw ParserException("Unrecognized profiling mode \"%s\", supported formats: [standard, detailed]", parameter);
	}
}

Value ProfilingModeSetting::GetSetting(ClientContext &context) {
	return Value(context.profiler->IsDetailedEnabled() ? "detailed" : "standard");
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void SchemaSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	context.catalog_search_path->Set(parameter, true);
}

Value SchemaSetting::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Search Path
//===--------------------------------------------------------------------===//
void SearchPathSetting::SetLocal(ClientContext &context, const Value &input) {
	auto parameter = input.ToString();
	context.catalog_search_path->Set(parameter, false);
}

Value SearchPathSetting::GetSetting(ClientContext &context) {
	return Value(StringUtil::Join(context.catalog_search_path->Get(), ","));
}

//===--------------------------------------------------------------------===//
// Set Profiler History Size
//===--------------------------------------------------------------------===//
void SetProfilerHistorySize::SetLocal(ClientContext &context, const Value &input) {
	auto size = input.GetValue<int64_t>();
	if (size <= 0) {
		throw ParserException("Size should be >= 0");
	}
	context.query_profiler_history->SetProfilerHistorySize(size);
}

Value SetProfilerHistorySize::GetSetting(ClientContext &context) {
	return Value();
}

//===--------------------------------------------------------------------===//
// Threads Setting
//===--------------------------------------------------------------------===//
void ThreadsSetting::SetGlobal(DatabaseInstance *db, DBConfig &config, const Value &input) {
	config.maximum_threads = input.GetValue<int64_t>();
	if (db) {
		TaskScheduler::GetScheduler(*db).SetThreads(config.maximum_threads);
	}
}

Value ThreadsSetting::GetSetting(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	return Value::BIGINT(config.maximum_threads);
}


}
