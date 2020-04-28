#include "duckdb/execution/operator/helper/physical_pragma.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/planner/expression_binder.hpp"

#include <cctype>

using namespace duckdb;
using namespace std;

static idx_t ParseMemoryLimit(string arg);

void PhysicalPragma::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) {
	auto &pragma = *info;
	auto &keyword = pragma.name;
	if (keyword == "enable_profile" || keyword == "enable_profiling") {
		// enable profiling
		if (pragma.pragma_type == PragmaType::ASSIGNMENT) {
			// enable_profiling with assignment
			// this is either enable_profiling = json, or enable_profiling = query_tree
			string assignment = pragma.parameters[0].ToString();
			if (assignment == "json") {
				context.profiler.automatic_print_format = ProfilerPrintFormat::JSON;
			} else if (assignment == "query_tree") {
				context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
			} else {
				throw ParserException("Unrecognized print format %s, supported formats: [json, query_tree]",
				                      assignment.c_str());
			}
		} else if (pragma.pragma_type == PragmaType::NOTHING) {
			context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
		} else {
			throw ParserException("Cannot call PRAGMA enable_profiling");
		}
		context.profiler.Enable();
	} else if (keyword == "disable_profile" || keyword == "disable_profiling") {
		if (pragma.pragma_type != PragmaType::NOTHING) {
			throw ParserException("disable_profiling cannot take parameters!");
		}
		// enable profiling
		context.profiler.Disable();
		context.profiler.automatic_print_format = ProfilerPrintFormat::NONE;
	} else if (keyword == "profiling_output" || keyword == "profile_output") {
		// set file location of where to save profiling output
		if (pragma.pragma_type != PragmaType::ASSIGNMENT || pragma.parameters[0].type != TypeId::VARCHAR) {
			throw ParserException(
			    "Profiling output must be an assignment (e.g. PRAGMA profile_output='/tmp/test.json')");
		}
		context.profiler.save_location = pragma.parameters[0].str_value;
	} else if (keyword == "memory_limit") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Memory limit must be an assignment (e.g. PRAGMA memory_limit='1GB')");
		}
		if (pragma.parameters[0].type == TypeId::VARCHAR) {
			idx_t new_limit = ParseMemoryLimit(pragma.parameters[0].str_value);
			// set the new limit in the buffer manager
			context.db.storage->buffer_manager->SetLimit(new_limit);
		} else {
			int64_t value = pragma.parameters[0].GetValue<int64_t>();
			if (value < 0) {
				// limit < 0, set limit to infinite
				context.db.storage->buffer_manager->SetLimit();
			} else {
				throw ParserException(
				    "Memory limit must be an assignment with a memory unit (e.g. PRAGMA memory_limit='1GB')");
			}
		}
	} else if (keyword == "collation" || keyword == "default_collation") {
		if (pragma.pragma_type != PragmaType::ASSIGNMENT) {
			throw ParserException("Collation must be an assignment (e.g. PRAGMA default_collation=NOCASE)");
		}
		auto collation_param = StringUtil::Lower(pragma.parameters[0].CastAs(TypeId::VARCHAR).str_value);
		// bind the collation to verify that it exists
		ExpressionBinder::PushCollation(context, nullptr, collation_param);
		context.db.collation = collation_param;
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
