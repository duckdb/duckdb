#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace regexp_util {

bool TryParseConstantPattern(ClientContext &context, Expression &expr, string &constant_string) {
	if (!expr.IsFoldable()) {
		return false;
	}
	Value pattern_str = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!pattern_str.IsNull() && pattern_str.type().id() == LogicalTypeId::VARCHAR) {
		constant_string = StringValue::Get(pattern_str);
		return true;
	}
	return false;
}

void ParseRegexOptions(const string &options, duckdb_re2::RE2::Options &result, bool *global_replace) {
	for (idx_t i = 0; i < options.size(); i++) {
		switch (options[i]) {
		case 'c':
			// case-sensitive matching
			result.set_case_sensitive(true);
			break;
		case 'i':
			// case-insensitive matching
			result.set_case_sensitive(false);
			break;
		case 'l':
			// literal matching
			result.set_literal(true);
			break;
		case 'm':
		case 'n':
		case 'p':
			// newline-sensitive matching
			result.set_dot_nl(false);
			break;
		case 's':
			// non-newline-sensitive matching
			result.set_dot_nl(true);
			break;
		case 'g':
			// global replace, only available for regexp_replace
			if (global_replace) {
				*global_replace = true;
			} else {
				throw InvalidInputException("Option 'g' (global replace) is only valid for regexp_replace");
			}
			break;
		case ' ':
		case '\t':
		case '\n':
			// ignore whitespace
			break;
		default:
			throw InvalidInputException("Unrecognized Regex option %c", options[i]);
		}
	}
}

void ParseRegexOptions(ClientContext &context, Expression &expr, RE2::Options &target, bool *global_replace) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!expr.IsFoldable()) {
		throw InvalidInputException("Regex options field must be a constant");
	}
	Value options_str = ExpressionExecutor::EvaluateScalar(context, expr);
	if (options_str.IsNull()) {
		throw InvalidInputException("Regex options field must not be NULL");
	}
	if (options_str.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Regex options field must be a string");
	}
	ParseRegexOptions(StringValue::Get(options_str), target, global_replace);
}

} // namespace regexp_util

} // namespace duckdb
