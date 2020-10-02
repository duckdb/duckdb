#include "duckdb/function/pragma/pragma_functions.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <cctype>

namespace duckdb {

static void pragma_enable_profiling_statement(ClientContext &context, vector<Value> parameters) {
	context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
	context.profiler.Enable();
}

static void pragma_enable_profiling_assignment(ClientContext &context, vector<Value> parameters) {
	// this is either enable_profiling = json, or enable_profiling = query_tree
	string assignment = parameters[0].ToString();
	if (assignment == "json") {
		context.profiler.automatic_print_format = ProfilerPrintFormat::JSON;
	} else if (assignment == "query_tree") {
		context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
	} else {
		throw ParserException("Unrecognized print format %s, supported formats: [json, query_tree]", assignment);
	}
	context.profiler.Enable();
}

void register_enable_profiling(BuiltinFunctions &set) {
	vector<PragmaFunction> functions;
	functions.push_back(PragmaFunction::PragmaStatement(string(), pragma_enable_profiling_statement));
	functions.push_back(
	    PragmaFunction::PragmaAssignment(string(), pragma_enable_profiling_assignment, LogicalType::VARCHAR));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void pragma_disable_profiling(ClientContext &context, vector<Value> parameters) {
	context.profiler.Disable();
	context.profiler.automatic_print_format = ProfilerPrintFormat::NONE;
}

static void pragma_profile_output(ClientContext &context, vector<Value> parameters) {
	context.profiler.save_location = parameters[0].ToString();
}

static idx_t ParseMemoryLimit(string arg);

static void pragma_memory_limit(ClientContext &context, vector<Value> parameters) {
	idx_t new_limit = ParseMemoryLimit(parameters[0].ToString());
	// set the new limit in the buffer manager
	context.db.storage->buffer_manager->SetLimit(new_limit);
}

static void pragma_collation(ClientContext &context, vector<Value> parameters) {
	auto collation_param = StringUtil::Lower(parameters[0].ToString());
	// bind the collation to verify that it exists
	ExpressionBinder::TestCollation(context, collation_param);
	auto &config = DBConfig::GetConfig(context);
	config.collation = collation_param;
}

static void pragma_null_order(ClientContext &context, vector<Value> parameters) {
	auto &config = DBConfig::GetConfig(context);
	string new_null_order = StringUtil::Lower(parameters[0].ToString());
	if (new_null_order == "nulls first" || new_null_order == "null first" || new_null_order == "first") {
		config.default_null_order = OrderByNullType::NULLS_FIRST;
	} else if (new_null_order == "nulls last" || new_null_order == "null last" || new_null_order == "last") {
		config.default_null_order = OrderByNullType::NULLS_LAST;
	} else {
		throw ParserException("Unrecognized null order '%s', expected either NULLS FIRST or NULLS LAST",
		                      new_null_order);
	}
}

static void pragma_default_order(ClientContext &context, vector<Value> parameters) {
	auto &config = DBConfig::GetConfig(context);
	string new_order = StringUtil::Lower(parameters[0].ToString());
	if (new_order == "ascending" || new_order == "asc") {
		config.default_order_type = OrderType::ASCENDING;
	} else if (new_order == "descending" || new_order == "desc") {
		config.default_order_type = OrderType::DESCENDING;
	} else {
		throw ParserException("Unrecognized order order '%s', expected either ASCENDING or DESCENDING", new_order);
	}
}

static void pragma_set_threads(ClientContext &context, vector<Value> parameters) {
	auto nr_threads = parameters[0].GetValue<int64_t>();
	TaskScheduler::GetScheduler(context).SetThreads(nr_threads);
}

static void pragma_enable_verification(ClientContext &context, vector<Value> parameters) {
	context.query_verification_enabled = true;
}

static void pragma_disable_verification(ClientContext &context, vector<Value> parameters) {
	context.query_verification_enabled = false;
}

static void pragma_enable_force_parallelism(ClientContext &context, vector<Value> parameters) {
	context.force_parallelism = true;
}

static void pragma_disable_force_parallelism(ClientContext &context, vector<Value> parameters) {
	context.force_parallelism = false;
}

static void pragma_log_query_path(ClientContext &context, vector<Value> parameters) {
	auto str_val = parameters[0].ToString();
	if (str_val.empty()) {
		// empty path: clean up query writer
		context.log_query_writer = nullptr;
	} else {
		context.log_query_writer = make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(context), str_val);
	}
}

static void pragma_explain_output(ClientContext &context, vector<Value> parameters) {
	string val = parameters[0].ToString();
	if (val == "optimized") {
		context.explain_output_optimized_only = true;
	}else if (val == "physical"){
		context.explain_output_physical_only = true;
	}
	else if (val == "all") {
		context.explain_output_optimized_only = true;
	} else {
		throw ParserException("Expected PRAGMA explain_output={optimized, all}");
	}
}

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	register_enable_profiling(set);

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", pragma_disable_profiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", pragma_disable_profiling));

	set.AddFunction(PragmaFunction::PragmaAssignment("profile_output", pragma_profile_output, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("profiling_output", pragma_profile_output, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("memory_limit", pragma_memory_limit, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("collation", pragma_collation, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_collation", pragma_collation, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("null_order", pragma_null_order, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_null_order", pragma_null_order, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("order", pragma_default_order, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("default_order", pragma_default_order, LogicalType::VARCHAR));

	set.AddFunction(PragmaFunction::PragmaAssignment("threads", pragma_set_threads, LogicalType::BIGINT));
	set.AddFunction(PragmaFunction::PragmaAssignment("worker_threads", pragma_set_threads, LogicalType::BIGINT));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", pragma_enable_verification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", pragma_disable_verification));

	set.AddFunction(PragmaFunction::PragmaStatement("force_parallelism", pragma_enable_force_parallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_force_parallelism", pragma_disable_force_parallelism));

	set.AddFunction(PragmaFunction::PragmaAssignment("log_query_path", pragma_log_query_path, LogicalType::VARCHAR));
	set.AddFunction(PragmaFunction::PragmaAssignment("explain_output", pragma_explain_output, LogicalType::VARCHAR));
}

idx_t ParseMemoryLimit(string arg) {
	if (arg[0] == '-' || arg == "null" || arg == "none") {
		return INVALID_INDEX;
	}
	// split based on the number/non-number
	idx_t idx = 0;
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t num_start = idx;
	while ((arg[idx] >= '0' && arg[idx] <= '9') || arg[idx] == '.' || arg[idx] == 'e' || arg[idx] == 'E' ||
	       arg[idx] == '-') {
		idx++;
	}
	if (idx == num_start) {
		throw ParserException("Memory limit must have a number (e.g. PRAGMA memory_limit=1GB");
	}
	string number = arg.substr(num_start, idx - num_start);

	// try to parse the number
	double limit = Cast::Operation<string_t, double>(number.c_str());

	// now parse the memory limit unit (e.g. bytes, gb, etc)
	while (StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	idx_t start = idx;
	while (idx < arg.size() && !StringUtil::CharacterIsSpace(arg[idx])) {
		idx++;
	}
	if (limit < 0) {
		// limit < 0, set limit to infinite
		return (idx_t)-1;
	}
	string unit = StringUtil::Lower(arg.substr(start, idx - start));
	idx_t multiplier;
	if (unit == "byte" || unit == "bytes" || unit == "b") {
		multiplier = 1;
	} else if (unit == "kilobyte" || unit == "kilobytes" || unit == "kb" || unit == "k") {
		multiplier = 1000LL;
	} else if (unit == "megabyte" || unit == "megabytes" || unit == "mb" || unit == "m") {
		multiplier = 1000LL * 1000LL;
	} else if (unit == "gigabyte" || unit == "gigabytes" || unit == "gb" || unit == "g") {
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (unit == "terabyte" || unit == "terabytes" || unit == "tb" || unit == "t") {
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	} else {
		throw ParserException("Unknown unit for memory_limit: %s (expected: b, mb, gb or tb)", unit);
	}
	return (idx_t)multiplier * limit;
}

} // namespace duckdb
