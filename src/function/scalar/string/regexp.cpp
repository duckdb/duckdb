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

static bool RegexOptionsEquals(const duckdb_re2::RE2::Options &opt_a, const duckdb_re2::RE2::Options &opt_b) {
	return opt_a.case_sensitive() == opt_b.case_sensitive();
}

RegexpBaseBindData::RegexpBaseBindData() : constant_pattern(false) {
}
RegexpBaseBindData::RegexpBaseBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                       bool constant_pattern)
    : options(options), constant_string(move(constant_string_p)), constant_pattern(constant_pattern) {
}

RegexpBaseBindData::~RegexpBaseBindData() {
}

bool RegexpBaseBindData::Equals(const FunctionData &other_p) const {
	auto &other = (RegexpBaseBindData &)other_p;
	return constant_pattern == other.constant_pattern && constant_string == other.constant_string &&
	       RegexOptionsEquals(options, other.options);
}

static inline duckdb_re2::StringPiece CreateStringPiece(string_t &input) {
	return duckdb_re2::StringPiece(input.GetDataUnsafe(), input.GetSize());
}

struct RegexLocalState : public FunctionLocalState {
	explicit RegexLocalState(RegexpBaseBindData &info)
	    : constant_pattern(duckdb_re2::StringPiece(info.constant_string.c_str(), info.constant_string.size()),
	                       info.options) {
		D_ASSERT(info.constant_pattern);
	}

	RE2 constant_pattern;
};

static unique_ptr<FunctionLocalState> RegexInitLocalState(const BoundFunctionExpression &expr,
                                                          FunctionData *bind_data) {
	auto &info = (RegexpBaseBindData &)*bind_data;
	if (info.constant_pattern) {
		return make_unique<RegexLocalState>(info);
	}
	return nullptr;
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

void ParseRegexOptions(Expression &expr, RE2::Options &target, bool *global_replace = nullptr) {
	if (expr.HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!expr.IsFoldable()) {
		throw InvalidInputException("Regex options field must be a constant");
	}
	Value options_str = ExpressionExecutor::EvaluateScalar(expr);
	if (!options_str.IsNull() && options_str.type().id() == LogicalTypeId::VARCHAR) {
		ParseRegexOptions(StringValue::Get(options_str), target, global_replace);
	}
}

static bool TryParseConstantPattern(Expression &expr, string &constant_string) {
	if (!expr.IsFoldable()) {
		return false;
	}
	Value pattern_str = ExpressionExecutor::EvaluateScalar(expr);
	if (!pattern_str.IsNull() && pattern_str.type().id() == LogicalTypeId::VARCHAR) {
		constant_string = StringValue::Get(pattern_str);
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Regexp Matches
//===--------------------------------------------------------------------===//
RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern)
    : RegexpBaseBindData(options, move(constant_string_p), constant_pattern) {
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

RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern, string range_min_p, string range_max_p,
                                             bool range_success)
    : RegexpBaseBindData(options, move(constant_string_p), constant_pattern), range_min(move(range_min_p)),
      range_max(move(range_max_p)), range_success(range_success) {
}

unique_ptr<FunctionData> RegexpMatchesBindData::Copy() const {
	return make_unique<RegexpMatchesBindData>(options, constant_string, constant_pattern, range_min, range_max,
	                                          range_success);
}

static unique_ptr<FunctionData> RegexpMatchesBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	RE2::Options options;
	options.set_log_errors(false);
	if (arguments.size() == 3) {
		ParseRegexOptions(*arguments[2], options);
	}

	string constant_string;
	bool constant_pattern;
	constant_pattern = TryParseConstantPattern(*arguments[1], constant_string);
	return make_unique<RegexpMatchesBindData>(options, move(constant_string), constant_pattern);
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

//===--------------------------------------------------------------------===//
// Regexp Replace
//===--------------------------------------------------------------------===//
RegexpReplaceBindData::RegexpReplaceBindData() : global_replace(false) {
}

RegexpReplaceBindData::RegexpReplaceBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern, bool global_replace)
    : RegexpBaseBindData(options, move(constant_string_p), constant_pattern), global_replace(global_replace) {
}

unique_ptr<FunctionData> RegexpReplaceBindData::Copy() const {
	auto copy = make_unique<RegexpReplaceBindData>(options, constant_string, constant_pattern, global_replace);
	return move(copy);
}

bool RegexpReplaceBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const RegexpReplaceBindData &)other_p;
	return RegexpBaseBindData::Equals(other) && global_replace == other.global_replace;
}

static unique_ptr<FunctionData> RegexReplaceBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto data = make_unique<RegexpReplaceBindData>();

	data->constant_pattern = TryParseConstantPattern(*arguments[1], data->constant_string);
	if (arguments.size() == 4) {
		ParseRegexOptions(*arguments[3], data->options, &data->global_replace);
	}
	data->options.set_log_errors(false);
	return move(data);
}

static void RegexReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpReplaceBindData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	auto &replaces = args.data[2];

	if (info.constant_pattern) {
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);
		BinaryExecutor::Execute<string_t, string_t, string_t>(
		    strings, replaces, result, args.size(), [&](string_t input, string_t replace) {
			    std::string sstring = input.GetString();
			    if (info.global_replace) {
				    RE2::GlobalReplace(&sstring, lstate.constant_pattern, CreateStringPiece(replace));
			    } else {
				    RE2::Replace(&sstring, lstate.constant_pattern, CreateStringPiece(replace));
			    }
			    return StringVector::AddString(result, sstring);
		    });
	} else {
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
}

//===--------------------------------------------------------------------===//
// Regexp Extract
//===--------------------------------------------------------------------===//
RegexpExtractBindData::RegexpExtractBindData() {
}

RegexpExtractBindData::RegexpExtractBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern, string group_string_p)
    : RegexpBaseBindData(options, move(constant_string_p), constant_pattern), group_string(move(group_string_p)),
      rewrite(group_string) {
}

unique_ptr<FunctionData> RegexpExtractBindData::Copy() const {
	return make_unique<RegexpExtractBindData>(options, constant_string, constant_pattern, group_string);
}

bool RegexpExtractBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const RegexpExtractBindData &)other_p;
	return RegexpBaseBindData::Equals(other) && group_string == other.group_string;
}

static unique_ptr<FunctionData> RegexExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);

	duckdb_re2::RE2::Options options;

	string constant_string;
	bool constant_pattern = TryParseConstantPattern(*arguments[1], constant_string);

	string group_string = "";
	if (arguments.size() >= 3) {
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
	if (arguments.size() >= 4) {
		ParseRegexOptions(*arguments[3], options);
	}
	return make_unique<RegexpExtractBindData>(options, move(constant_string), constant_pattern, move(group_string));
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
			                                                      RE2 re(CreateStringPiece(pattern), info.options);
			                                                      return Extract(input, result, re, info.rewrite);
		                                                      });
	}
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
	                                          LogicalType::VARCHAR, RegexReplaceFunction, RegexReplaceBind, nullptr,
	                                          nullptr, RegexInitLocalState));
	regexp_replace.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    RegexReplaceFunction, RegexReplaceBind, nullptr, nullptr, RegexInitLocalState));

	ScalarFunctionSet regexp_extract("regexp_extract");
	regexp_extract.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, RegexExtractFunction,
	                   RegexExtractBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	                   FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::VARCHAR, RegexExtractFunction,
	    RegexExtractBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    RegexExtractFunction, RegexExtractBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING));

	set.AddFunction(regexp_full_match);
	set.AddFunction(regexp_partial_match);
	set.AddFunction(regexp_replace);
	set.AddFunction(regexp_extract);
}

} // namespace duckdb
