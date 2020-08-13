#include "duckdb/execution/operator/helper/physical_pragma.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <cctype>

using namespace std;

namespace duckdb {

static idx_t ParseMemoryLimit(string arg);

void PhysicalPragma::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &client = context.client;
	auto &pragma = *info;
	auto &keyword = pragma.name;
	if (keyword == "enable_profile" || keyword == "enable_profiling") {
		// enable profiling
		if (pragma.pragma_type == PragmaType::ASSIGNMENT) {
			// enable_profiling with assignment
			// this is either enable_profiling = json, or enable_profiling = query_tree
			string assignment = pragma.parameters[0].ToString();
			if (assignment == "json") {
				client.profiler.automatic_print_format = ProfilerPrintFormat::JSON;
			} else if (assignment == "query_tree") {
				client.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
			} else {
				throw ParserException("Unrecognized print format %s, supported formats: [json, query_tree]",
				                      assignment.c_str());
			}
		} else if (pragma.pragma_type == PragmaType::NOTHING) {
			client.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
		} else {
			throw ParserException("Cannot call PRAGMA enable_profiling");
		}
		client.profiler.Enable();
	} else if (keyword == "disable_profile" || keyword == "disable_profiling") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("disable_profiling cannot take parameters!");
		}
		// enable profiling
		client.profiler.Disable();
		client.profiler.automatic_print_format = ProfilerPrintFormat::NONE;
	} else if (keyword == "profiling_output" || keyword == "profile_output") {
		// set file location of where to save profiling output
		if (pragma.pragma_type != PragmaType::ASSIGNMENT || pragma.parameters[0].type().id() != LogicalTypeId::VARCHAR) {
			throw ParserException(
			    "Profiling output must be an assignment (e.g. PRAGMA profile_output='/tmp/test.json')");
		}
		client.profiler.save_location = pragma.parameters[0].str_value;
	} else if (keyword == "memory_limit") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Memory limit must be an assignment (e.g. PRAGMA memory_limit='1GB')");
		}
		if (pragma.parameters[0].type().id() == LogicalTypeId::VARCHAR) {
			idx_t new_limit = ParseMemoryLimit(pragma.parameters[0].str_value);
			// set the new limit in the buffer manager
			client.db.storage->buffer_manager->SetLimit(new_limit);
		} else {
			int64_t value = pragma.parameters[0].GetValue<int64_t>();
			if (value < 0) {
				// limit < 0, set limit to infinite
				client.db.storage->buffer_manager->SetLimit();
			} else {
				throw ParserException(
				    "Memory limit must be an assignment with a memory unit (e.g. PRAGMA memory_limit='1GB')");
			}
		}
	} else if (keyword == "collation" || keyword == "default_collation") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Collation must be an assignment (e.g. PRAGMA default_collation=NOCASE)");
		}
		auto collation_param = StringUtil::Lower(pragma.parameters[0].ToString());
		// bind the collation to verify that it exists
		ExpressionBinder::PushCollation(client, nullptr, collation_param);
		auto &config = DBConfig::GetConfig(client);
		config.collation = collation_param;
	} else if (keyword == "null_order" || keyword == "default_null_order") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Null order must be an assignment (e.g. PRAGMA default_null_order='NULLS FIRST')");
		}
		auto &config = DBConfig::GetConfig(client);
		string new_null_order = StringUtil::Lower(pragma.parameters[0].ToString());
		if (new_null_order == "nulls first" || new_null_order == "null first" || new_null_order == "first") {
			config.default_null_order = OrderByNullType::NULLS_FIRST;
		} else if (new_null_order == "nulls last" || new_null_order == "null last" || new_null_order == "last") {
			config.default_null_order = OrderByNullType::NULLS_LAST;
		} else {
			throw ParserException("Unrecognized null order '%s', expected either NULLS FIRST or NULLS LAST",
			                      new_null_order.c_str());
		}
	} else if (keyword == "order" || keyword == "default_order") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Order must be an assignment (e.g. PRAGMA default_order=DESCENDING)");
		}
		auto &config = DBConfig::GetConfig(client);
		string new_order = StringUtil::Lower(pragma.parameters[0].ToString());
		if (new_order == "ascending" || new_order == "asc") {
			config.default_order_type = OrderType::ASCENDING;
		} else if (new_order == "descending" || new_order == "desc") {
			config.default_order_type = OrderType::DESCENDING;
		} else {
			throw ParserException("Unrecognized order order '%s', expected either ASCENDING or DESCENDING",
			                      new_order.c_str());
		}
	} else if (keyword == "threads" || keyword == "worker_threads") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Order must be an assignment (e.g. PRAGMA threads=4)");
		}
		auto nr_threads = pragma.parameters[0].GetValue<int64_t>();
		TaskScheduler::GetScheduler(client).SetThreads(nr_threads);
	} else if (keyword == "enable_verification") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Enable verification must be a statement (PRAGMA enable_verification)");
		}
		context.client.query_verification_enabled = true;
	} else if (keyword == "disable_verification") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Disable verification must be a statement (PRAGMA disable_verification)");
		}
		context.client.query_verification_enabled = false;
	} else if (keyword == "force_parallelism") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Force parallelism must be a statement (PRAGMA force_parallelism)");
		}
		context.client.force_parallelism = true;
	}  else if (keyword == "disable_force_parallelism") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("Disable force parallelism must be a statement (PRAGMA disable_force_parallelism)");
		}
		context.client.force_parallelism = false;
	} else if (keyword == "log_query_path") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Log query path must be an assignment (PRAGMA log_query_path='/path/to/file') (or empty to disable)");
		}
		auto str_val = pragma.parameters[0].ToString();
		if (str_val.empty()) {
			// empty path: clean up query writer
			context.client.log_query_writer = nullptr;
		} else {
			context.client.log_query_writer = make_unique<BufferedFileWriter>(FileSystem::GetFileSystem(context.client), str_val);
		}
	} else if (keyword == "explain_output") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Explain output must be an assignment (e.g. PRAGMA explain_output='optimized')");
		}
		string val = pragma.parameters[0].ToString();
		if (val == "optimized") {
			context.client.explain_output_optimized_only = true;
		} else if (val == "all") {
			context.client.explain_output_optimized_only = true;
		} else {
			throw ParserException("Expected PRAGMA explain_output={optimized, all}");
		}
	} else {
		throw ParserException("Unrecognized PRAGMA keyword: %s", keyword.c_str());
	}
}

idx_t ParseMemoryLimit(string arg) {
	// split based on the number/non-number
	idx_t idx = 0;
	while (std::isspace(arg[idx])) {
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
	while (std::isspace(arg[idx])) {
		idx++;
	}
	idx_t start = idx;
	while (idx < arg.size() && !std::isspace(arg[idx])) {
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
		throw ParserException("Unknown unit for memory_limit: %s (expected: b, mb, gb or tb)", unit.c_str());
	}
	return (idx_t)multiplier * limit;
}

} // namespace duckdb
