#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "utf8proc_wrapper.hpp"

#include "duckdb/function/scalar/regexp.hpp"

using namespace std;

namespace duckdb {

RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, unique_ptr<duckdb_re2::RE2> constant_pattern, string range_min, string range_max,
                                             bool range_success)
    : options(move(options)), constant_pattern(std::move(constant_pattern)), range_min(range_min), range_max(range_max),
      range_success(range_success) {
}

RegexpMatchesBindData::~RegexpMatchesBindData() {
}

unique_ptr<FunctionData> RegexpMatchesBindData::Copy() {
	return make_unique<RegexpMatchesBindData>(options, move(constant_pattern), range_min, range_max, range_success);
}

static inline duckdb_re2::StringPiece CreateStringPiece(string_t &input) {
	return duckdb_re2::StringPiece(input.GetData(), input.GetSize());
}

static void ParseRegexOptions(string &options, duckdb_re2::RE2::Options &result, bool *global_replace = nullptr) {
	for(idx_t i = 0; i < options.size(); i++) {
		switch(options[i]) {
		case 'c':
			// case-sensitive matching
			result.set_case_sensitive(true);
			break;
		case 'i':
			// case-insensitive matching
			result.set_case_sensitive(false);
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

struct RegexPartialMatch {
	static inline bool Operation(const duckdb_re2::StringPiece &input, duckdb_re2::RE2 &re) {
		return duckdb_re2::RE2::PartialMatch(input, re);
	}
};

struct RegexFullMatch {
	static inline bool Operation(const duckdb_re2::StringPiece &input, duckdb_re2::RE2 &re) {
		return duckdb_re2::RE2::FullMatch(input, re);
	}
};

template <class OP> static void regexp_matches_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpMatchesBindData &)*func_expr.bind_info;

	if (info.constant_pattern) {
		UnaryExecutor::Execute<string_t, bool, true>(strings, result, args.size(), [&](string_t input) {
			return OP::Operation(CreateStringPiece(input), *info.constant_pattern);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool, true>(strings, patterns, result, args.size(),
		                                                        [&](string_t input, string_t pattern) {
			                                                        RE2 re(CreateStringPiece(pattern), info.options);
			                                                        if (!re.ok()) {
				                                                        throw Exception(re.error());
			                                                        }
			                                                        return OP::Operation(CreateStringPiece(input), re);
		                                                        });
	}
}

static unique_ptr<FunctionData> regexp_matches_get_bind_function(BoundFunctionExpression &expr,
                                                                 ClientContext &context) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	assert(expr.children.size() == 2 || expr.children.size() == 3);
	RE2::Options options;
	options.set_log_errors(false);
	if (expr.children.size() == 3) {
		if (!expr.children[2]->IsScalar()) {
			throw InvalidInputException("Regex options field must be a constant");
		}
		Value options_str = ExpressionExecutor::EvaluateScalar(*expr.children[2]);
		if (!options_str.is_null && options_str.type == PhysicalType::VARCHAR) {
			ParseRegexOptions(options_str.str_value, options);
		}
	}

	if (expr.children[1]->IsScalar()) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*expr.children[1]);
		if (!pattern_str.is_null && pattern_str.type == PhysicalType::VARCHAR) {
			auto re = make_unique<RE2>(pattern_str.str_value, options);
			if (!re->ok()) {
				throw Exception(re->error());
			}

			string range_min, range_max;
			auto range_success = re->PossibleMatchRange(&range_min, &range_max, 1000);
			return make_unique<RegexpMatchesBindData>(move(options), move(re), range_min, range_max, range_success);
		}
	}
	return make_unique<RegexpMatchesBindData>(move(options), nullptr, "", "", false);
}

static void regexp_replace_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpReplaceBindData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	auto &replaces = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    strings, patterns, replaces, result, args.size(), [&](string_t input, string_t pattern, string_t replace) {
		    RE2 re(CreateStringPiece(pattern), info.options);
		    std::string sstring(input.GetData(), input.GetSize());
			if (info.global_replace) {
				RE2::GlobalReplace(&sstring, re, CreateStringPiece(replace));
			} else {
				RE2::Replace(&sstring, re, CreateStringPiece(replace));
			}
			return StringVector::AddString(result, sstring);
	    });
}

unique_ptr<FunctionData> RegexpReplaceBindData::Copy() {
	auto copy = make_unique<RegexpReplaceBindData>();
	copy->options = options;
	copy->global_replace = global_replace;
	return move(copy);
}

static unique_ptr<FunctionData> regexp_replace_bind_function(BoundFunctionExpression &expr, ClientContext &context) {
	auto data = make_unique<RegexpReplaceBindData>();
	data->options.set_log_errors(false);
	if (expr.children.size() == 4) {
		if (!expr.children[3]->IsScalar()) {
			throw InvalidInputException("Regex options field must be a constant");
		}
		Value options_str = ExpressionExecutor::EvaluateScalar(*expr.children[3]);
		if (!options_str.is_null && options_str.type == PhysicalType::VARCHAR) {
			ParseRegexOptions(options_str.str_value, data->options, &data->global_replace);
		}
	}

	return move(data);
}

void RegexpFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet regexp_full_match("regexp_full_match");
	regexp_full_match.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               regexp_matches_function<RegexFullMatch>, false, regexp_matches_get_bind_function));
	regexp_full_match.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               regexp_matches_function<RegexFullMatch>, false, regexp_matches_get_bind_function));

	ScalarFunctionSet regexp_partial_match("regexp_matches");
	regexp_partial_match.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               regexp_matches_function<RegexPartialMatch>, false,
	                               regexp_matches_get_bind_function));
	regexp_partial_match.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                               regexp_matches_function<RegexPartialMatch>, false,
	                               regexp_matches_get_bind_function));

	ScalarFunctionSet regexp_replace("regexp_replace");
	regexp_replace.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, regexp_replace_function, false, regexp_replace_bind_function));
	regexp_replace.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                               LogicalType::VARCHAR, regexp_replace_function, false, regexp_replace_bind_function));

	set.AddFunction(regexp_full_match);
	set.AddFunction(regexp_partial_match);
	set.AddFunction(regexp_replace);
}

} // namespace duckdb
