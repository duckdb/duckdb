#include "duckdb/function/scalar/regexp.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string_p)
    : options(options), constant_string(move(constant_string_p)) {
	constant_pattern = !constant_string.empty();
	if (constant_pattern) {
		auto pattern = make_unique<RE2>(constant_string, options);
		if (!pattern->ok()) {
			throw Exception(pattern->error());
		}

		range_success = pattern->PossibleMatchRange(&range_min, &range_max, 1000);
	} else {
		range_success = false;
	}
}

static bool RegexOptionsEquals(const duckdb_re2::RE2::Options &opt_a, const duckdb_re2::RE2::Options &opt_b) {
	return opt_a.case_sensitive() == opt_b.case_sensitive();
}

RegexpMatchesBindData::~RegexpMatchesBindData() {
}

unique_ptr<FunctionData> RegexpMatchesBindData::Copy() const {
	return make_unique<RegexpMatchesBindData>(options, constant_string);
}

bool RegexpMatchesBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const RegexpMatchesBindData &)other_p;
	return constant_string == other.constant_string && RegexOptionsEquals(options, other.options);
}

static inline duckdb_re2::StringPiece CreateStringPiece(string_t &input) {
	return duckdb_re2::StringPiece(input.GetDataUnsafe(), input.GetSize());
}

static void ParseRegexOptions(const string &options, duckdb_re2::RE2::Options &result, bool *global_replace = nullptr) {
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

struct RegexLocalState : public FunctionLocalState {
	explicit RegexLocalState(RegexpMatchesBindData &info)
	    : constant_pattern(duckdb_re2::StringPiece(info.constant_string.c_str(), info.constant_string.size()),
	                       info.options) {
		D_ASSERT(info.constant_pattern);
	}

	explicit RegexLocalState(RegexpExtractBindData &info)
	    : constant_pattern(duckdb_re2::StringPiece(info.constant_string.c_str(), info.constant_string.size())) {
		D_ASSERT(info.constant_pattern);
	}

	RE2 constant_pattern;
};

static unique_ptr<FunctionLocalState> RegexInitLocalState(const BoundFunctionExpression &expr,
                                                          FunctionData *bind_data) {
	auto &info = (RegexpMatchesBindData &)*bind_data;
	if (info.constant_pattern) {
		return make_unique<RegexLocalState>(info);
	}
	return nullptr;
}

template <class OP>
static void RegexpMatchesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &strings = args.data[0];
	auto &patterns = args.data[1];

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpMatchesBindData &)*func_expr.bind_info;

	if (info.constant_pattern) {
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);
		UnaryExecutor::Execute<string_t, bool>(strings, result, args.size(), [&](string_t input) {
			return OP::Operation(CreateStringPiece(input), lstate.constant_pattern);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool>(strings, patterns, result, args.size(),
		                                                  [&](string_t input, string_t pattern) {
			                                                  RE2 re(CreateStringPiece(pattern), info.options);
			                                                  if (!re.ok()) {
				                                                  throw Exception(re.error());
			                                                  }
			                                                  return OP::Operation(CreateStringPiece(input), re);
		                                                  });
	}
}

static unique_ptr<FunctionData> RegexpMatchesBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	RE2::Options options;
	options.set_log_errors(false);
	if (arguments.size() == 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw InvalidInputException("Regex options field must be a constant");
		}
		Value options_str = ExpressionExecutor::EvaluateScalar(*arguments[2]);
		if (!options_str.IsNull() && options_str.type().id() == LogicalTypeId::VARCHAR) {
			ParseRegexOptions(StringValue::Get(options_str), options);
		}
	}

	if (arguments[1]->IsFoldable()) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		if (!pattern_str.IsNull() && pattern_str.type().id() == LogicalTypeId::VARCHAR) {
			return make_unique<RegexpMatchesBindData>(options, StringValue::Get(pattern_str));
		}
	}
	return make_unique<RegexpMatchesBindData>(options, "");
}

static void RegexReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpReplaceBindData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	auto &replaces = args.data[2];

	TernaryExecutor::Execute<string_t, string_t, string_t, string_t>(
	    strings, patterns, replaces, result, args.size(), [&](string_t input, string_t pattern, string_t replace) {
		    RE2 re(CreateStringPiece(pattern), info.options);
		    std::string sstring = input.GetString();
		    if (info.global_replace) {
			    RE2::GlobalReplace(&sstring, re, CreateStringPiece(replace));
		    } else {
			    RE2::Replace(&sstring, re, CreateStringPiece(replace));
		    }
		    return StringVector::AddString(result, sstring);
	    });
}

unique_ptr<FunctionData> RegexpReplaceBindData::Copy() const {
	auto copy = make_unique<RegexpReplaceBindData>();
	copy->options = options;
	copy->global_replace = global_replace;
	return move(copy);
}

bool RegexpReplaceBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const RegexpReplaceBindData &)other_p;
	return global_replace == other.global_replace && RegexOptionsEquals(options, other.options);
}

static unique_ptr<FunctionData> RegexReplaceBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto data = make_unique<RegexpReplaceBindData>();
	data->options.set_log_errors(false);
	if (arguments.size() == 4) {
		if (arguments[3]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[3]->IsFoldable()) {
			throw InvalidInputException("Regex options field must be a constant");
		}
		Value options_str = ExpressionExecutor::EvaluateScalar(*arguments[3]);
		if (!options_str.IsNull() && options_str.type().id() == LogicalTypeId::VARCHAR) {
			ParseRegexOptions(StringValue::Get(options_str), data->options, &data->global_replace);
		}
	}

	return move(data);
}

inline static string_t Extract(const string_t &input, Vector &result, const RE2 &re,
                               const duckdb_re2::StringPiece &rewrite) {
	std::string extracted;
	RE2::Extract(input.GetString(), re, rewrite, &extracted);
	return StringVector::AddString(result, extracted.c_str(), extracted.size());
}

static void RegexExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (RegexpExtractBindData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	if (info.constant_pattern) {
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);
		UnaryExecutor::Execute<string_t, string_t>(strings, result, args.size(), [&](string_t input) {
			return Extract(input, result, lstate.constant_pattern, info.rewrite);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, string_t>(strings, patterns, result, args.size(),
		                                                      [&](string_t input, string_t pattern) {
			                                                      RE2 re(CreateStringPiece(pattern));
			                                                      return Extract(input, result, re, info.rewrite);
		                                                      });
	}
}

static unique_ptr<FunctionLocalState> RegexExtractInitLocalState(const BoundFunctionExpression &expr,
                                                                 FunctionData *bind_data) {
	auto &info = (RegexpExtractBindData &)*bind_data;
	if (info.constant_pattern) {
		return make_unique<RegexLocalState>(info);
	}
	return nullptr;
}

RegexpExtractBindData::RegexpExtractBindData(bool constant_pattern, const string &constant_string,
                                             const string &group_string_p)
    : constant_pattern(constant_pattern), constant_string(constant_string), group_string(group_string_p),
      rewrite(group_string) {
}

unique_ptr<FunctionData> RegexpExtractBindData::Copy() const {
	return make_unique<RegexpExtractBindData>(constant_pattern, constant_string, group_string);
}

bool RegexpExtractBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const RegexpExtractBindData &)other_p;
	return constant_string == other.constant_string && group_string == other.group_string;
}

static unique_ptr<FunctionData> RegexExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);

	bool constant_pattern = arguments[1]->IsFoldable();
	string pattern = "";
	if (constant_pattern) {
		Value pattern_str = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		if (!pattern_str.IsNull() && pattern_str.type().id() == LogicalTypeId::VARCHAR) {
			pattern = StringValue::Get(pattern_str);
		}
	}

	string group_string = "";
	if (arguments.size() == 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw InvalidInputException("Group index field field must be a constant!");
		}
		Value group = ExpressionExecutor::EvaluateScalar(*arguments[2]);
		if (!group.IsNull()) {
			auto group_idx = group.GetValue<int32_t>();
			if (group_idx < 0 || group_idx > 9) {
				throw InvalidInputException("Group index must be between 0 and 9!");
			}
			group_string = "\\" + to_string(group_idx);
		}
	} else {
		group_string = "\\0";
	}

	return make_unique<RegexpExtractBindData>(constant_pattern, pattern, group_string);
}

void RegexpFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet regexp_full_match("regexp_full_match");
	regexp_full_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, RegexpMatchesFunction<RegexFullMatch>,
	    RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_full_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	    RegexpMatchesFunction<RegexFullMatch>, RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState,
	    LogicalType::INVALID, FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));

	ScalarFunctionSet regexp_partial_match("regexp_matches");
	regexp_partial_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, RegexpMatchesFunction<RegexPartialMatch>,
	    RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_partial_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	    RegexpMatchesFunction<RegexPartialMatch>, RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState,
	    LogicalType::INVALID, FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));

	ScalarFunctionSet regexp_replace("regexp_replace");
	regexp_replace.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                          LogicalType::VARCHAR, RegexReplaceFunction, RegexReplaceBind));
	regexp_replace.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   LogicalType::VARCHAR, RegexReplaceFunction, RegexReplaceBind));

	ScalarFunctionSet regexp_extract("regexp_extract");
	regexp_extract.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, RegexExtractFunction,
	                   RegexExtractBind, nullptr, nullptr, RegexExtractInitLocalState, LogicalType::INVALID,
	                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR, RegexExtractFunction,
	    RegexExtractBind, nullptr, nullptr, RegexExtractInitLocalState, LogicalType::INVALID,
	    FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));

	set.AddFunction(regexp_full_match);
	set.AddFunction(regexp_partial_match);
	set.AddFunction(regexp_replace);
	set.AddFunction(regexp_extract);
}

} // namespace duckdb
