#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "function/scalar_function/regexp_matches.hpp"
#include "re2/re2.h"

using namespace std;

namespace duckdb {

void regexp_replace_function(ExpressionExecutor &exec, Vector inputs[], uint64_t input_count,
                             BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 3);
	auto &strings = inputs[0];
	auto &patterns = inputs[1];
	auto &replaces = inputs[2];

	assert(strings.type == TypeId::VARCHAR);
	assert(patterns.type == TypeId::VARCHAR);
	assert(replaces.type == TypeId::VARCHAR);

	result.Initialize(TypeId::VARCHAR);
	result.nullmask =
	    strings.nullmask | patterns.nullmask | replaces.nullmask; // TODO what would jesus, err postgres do

	auto strings_data = (const char **)strings.data;
	auto patterns_data = (const char **)patterns.data;
	auto replaces_data = (const char **)replaces.data;
	auto result_data = (const char **)result.data;

	RE2::Options options;
	options.set_log_errors(false);

	VectorOperations::TernaryExec(
	    strings, patterns, replaces, result,
	    [&](uint64_t strings_index, uint64_t patterns_index, uint64_t replaces_index, uint64_t result_index) {
		    if (result.nullmask[strings_index]) {
			    return;
		    }

		    auto string = strings_data[strings_index];
		    auto pattern = patterns_data[patterns_index];
		    auto replace = replaces_data[replaces_index];

		    RE2 re(pattern, options);
		    std::string sstring(string);
		    RE2::Replace(&sstring, re, replace);
		    result_data[result_index] = result.string_heap.AddString(sstring.c_str());
	    });
}

bool regexp_replace_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 3 && arguments[0].id == SQLTypeId::VARCHAR && arguments[1].id == SQLTypeId::VARCHAR &&
	       arguments[2].id == SQLTypeId::VARCHAR;
}

SQLType regexp_replace_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::VARCHAR);
}

} // namespace duckdb
