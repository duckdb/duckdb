#include "function/scalar_function/regexp_matches.hpp"

#include "common/exception.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "planner/expression/bound_function_expression.hpp"
#include "re2/re2.h"

using namespace std;

namespace duckdb {
namespace function {

struct RegexpMatchesBindData : public FunctionData {
	unique_ptr<RE2> constant_pattern;

	RegexpMatchesBindData(unique_ptr<RE2> constant_pattern) : constant_pattern(move(constant_pattern)) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<RegexpMatchesBindData>(move(constant_pattern));
	}
};

void regexp_matches_function(ExpressionExecutor &exec, Vector inputs[], size_t input_count,
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
	                             [&](size_t strings_index, size_t patterns_index, size_t result_index) {
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
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it.
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
			return make_unique<RegexpMatchesBindData>(move(re));
		}
	}
	return make_unique<RegexpMatchesBindData>(nullptr);
}

} // namespace function
} // namespace duckdb
