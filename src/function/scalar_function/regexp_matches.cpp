#include "function/scalar_function/regexp_matches.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "re2/re2.h"

using namespace std;

namespace duckdb {
namespace function {

void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], size_t input_count,
                             BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 2);
	auto &strings = inputs[0];
	auto &patterns = inputs[1];

	assert(strings.type == TypeId::VARCHAR);
	assert(patterns.type == TypeId::VARCHAR);

	result.Initialize(TypeId::BOOLEAN);
	result.nullmask = strings.nullmask | patterns.nullmask;

	auto strings_data = (const char **)strings.data;
	auto patterns_data = (const char **)patterns.data;
	auto result_data = (bool *)result.data;

	RE2::Options options;
	options.set_log_errors(false);

	VectorOperations::BinaryExec(strings, patterns, result,
	                             [&](size_t strings_index, size_t patterns_index, size_t result_index) {
		                             if (result.nullmask[result_index]) {
			                             return;
		                             }
		                             auto string = strings_data[strings_index];
		                             auto pattern = patterns_data[patterns_index];

		                             RE2 re(pattern, options);

		                             if (!re.ok()) {
			                             throw Exception(re.error());
		                             }
		                             // TODO if pattern is constant, create re object outside loop
		                             result_data[result_index] = RE2::PartialMatch(string, re);
	                             });
}

bool regexp_matches_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 2 && arguments[0].id == SQLTypeId::VARCHAR && arguments[1].id == SQLTypeId::VARCHAR;
}

SQLType regexp_matches_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::BOOLEAN);
}

} // namespace function
} // namespace duckdb
