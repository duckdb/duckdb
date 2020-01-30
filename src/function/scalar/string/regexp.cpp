#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"

#include "re2/re2.h"

using namespace std;

namespace duckdb {

RegexpMatchesBindData::RegexpMatchesBindData(unique_ptr<RE2> constant_pattern, string range_min, string range_max,
                                             bool range_success)
    : constant_pattern(std::move(constant_pattern)), range_min(range_min), range_max(range_max),
      range_success(range_success) {
}

RegexpMatchesBindData::~RegexpMatchesBindData() {
}

unique_ptr<FunctionData> RegexpMatchesBindData::Copy() {
	return make_unique<RegexpMatchesBindData>(move(constant_pattern), range_min, range_max, range_success);
}

static void regexp_matches_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpMatchesBindData &)*func_expr.bind_info;

	RE2::Options options;
	options.set_log_errors(false);

	if (info.constant_pattern) {
		// FIXME: this should be a unary loop
		BinaryExecutor::Execute<const char*, const char*, bool, true>(strings, patterns, result, [&](const char *string, const char *pattern) {
			return RE2::PartialMatch(string, *info.constant_pattern);
		});
	} else {
		BinaryExecutor::Execute<const char*, const char*, bool, true>(strings, patterns, result, [&](const char *string, const char *pattern) {
			RE2 re(pattern, options);
			if (!re.ok()) {
				throw Exception(re.error());
			}
			return RE2::PartialMatch(string, re);
		});
	}
}

static unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr,
                                                                 ClientContext &context) {
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
			// range_min and range_max might produce non-valid UTF8 strings, e.g. in the case of 'a.*'
			// in this case we don't push a range filter
			if (range_success && (!Value::IsUTF8String(range_min.c_str()) || !Value::IsUTF8String(range_max.c_str()))) {
				range_success = false;
			}

			return make_unique<RegexpMatchesBindData>(move(re), range_min, range_max, range_success);
		}
	}
	return make_unique<RegexpMatchesBindData>(nullptr, "", "", false);
}

static void regexp_replace_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	auto &replaces = args.data[2];

	RE2::Options options;
	options.set_log_errors(false);

	TernaryExecutor::Execute<const char *, const char *, const char *, const char *, true>(
	    strings, patterns, replaces, result,
	    [&](const char *string, const char *pattern, const char *replace) {
		    RE2 re(pattern, options);
		    std::string sstring(string);
		    RE2::Replace(&sstring, re, replace);
		    return result.AddString(sstring);
	    });
}

void RegexpFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("regexp_matches", {SQLType::VARCHAR, SQLType::VARCHAR}, SQLType::BOOLEAN,
	                               regexp_matches_function, false, regexp_matches_get_bind_function));
	set.AddFunction(ScalarFunction("regexp_replace", {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR},
	                               SQLType::VARCHAR, regexp_replace_function));
}

} // namespace duckdb
