#include "function/scalar_function/regexp_matches.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace std;

namespace duckdb {

void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], uint64_t input_count,
                             BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 2);
	auto &strings = inputs[0];
	auto &patterns = inputs[1];

	auto &info = (RegexpMatchesBindData &)*expr.bind_info;

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
	                             [&](uint64_t strings_index, uint64_t patterns_index, uint64_t result_index) {
		                             if (result.nullmask[result_index]) {
			                             return;
		                             }
		                             auto string = strings_data[strings_index];

		                             if (info.constant_pattern) {
			                             result_data[result_index] = RE2::PartialMatch(string, *info.constant_pattern);

		                             } else {
			                             auto pattern = patterns_data[patterns_index];
			                             RE2 re(pattern, options);

			                             if (!re.ok()) {
				                             throw Exception(re.error());
			                             }
			                             result_data[result_index] = RE2::PartialMatch(string, re);
		                             }
	                             });
}

bool regexp_matches_matches_arguments(vector<SQLType> &arguments) {
	return arguments.size() == 2 && arguments[0].id == SQLTypeId::VARCHAR && arguments[1].id == SQLTypeId::VARCHAR;
}

SQLType regexp_matches_get_return_type(vector<SQLType> &arguments) {
	return SQLType(SQLTypeId::BOOLEAN);
}

unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr, ClientContext &context) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	assert(expr.children.size() == 2);
	if (expr.children[1]->IsScalar()) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*expr.children[1]);
		if (!pattern_str.is_null && pattern_str.type == TypeId::VARCHAR) {
			RE2::Options options;
			options.set_log_errors(false);
			auto re = make_unique<RE2>(pattern_str.str_value, options);
			if (!re->ok()) {
				throw Exception(re->error());
			}

			string range_min, range_max;
			auto range_success = re->PossibleMatchRange(&range_min, &range_max, 1000);

			return make_unique<RegexpMatchesBindData>(move(re), range_min, range_max, range_success);
		}
	}
	return make_unique<RegexpMatchesBindData>(nullptr, "", "", false);
}

} // namespace duckdb
