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

using regexp_util::CreateStringPiece;
using regexp_util::Extract;
using regexp_util::ParseRegexOptions;
using regexp_util::TryParseConstantPattern;

static bool RegexOptionsEquals(const duckdb_re2::RE2::Options &opt_a, const duckdb_re2::RE2::Options &opt_b) {
	return opt_a.case_sensitive() == opt_b.case_sensitive();
}

RegexpBaseBindData::RegexpBaseBindData() : constant_pattern(false) {
}
RegexpBaseBindData::RegexpBaseBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                       bool constant_pattern)
    : options(options), constant_string(std::move(constant_string_p)), constant_pattern(constant_pattern) {
}

RegexpBaseBindData::~RegexpBaseBindData() {
}

bool RegexpBaseBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<RegexpBaseBindData>();
	return constant_pattern == other.constant_pattern && constant_string == other.constant_string &&
	       RegexOptionsEquals(options, other.options);
}

unique_ptr<FunctionLocalState> RegexInitLocalState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                   FunctionData *bind_data) {
	auto &info = bind_data->Cast<RegexpBaseBindData>();
	if (info.constant_pattern) {
		return make_uniq<RegexLocalState>(info);
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Regexp Matches
//===--------------------------------------------------------------------===//
RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern)
    : RegexpBaseBindData(options, std::move(constant_string_p), constant_pattern) {
	if (constant_pattern) {
		auto pattern = make_uniq<RE2>(constant_string, options);
		if (!pattern->ok()) {
			throw InvalidInputException(pattern->error());
		}

		range_success = pattern->PossibleMatchRange(&range_min, &range_max, 1000);
	} else {
		range_success = false;
	}
}

RegexpMatchesBindData::RegexpMatchesBindData(duckdb_re2::RE2::Options options, string constant_string_p,
                                             bool constant_pattern, string range_min_p, string range_max_p,
                                             bool range_success)
    : RegexpBaseBindData(options, std::move(constant_string_p), constant_pattern), range_min(std::move(range_min_p)),
      range_max(std::move(range_max_p)), range_success(range_success) {
}

unique_ptr<FunctionData> RegexpMatchesBindData::Copy() const {
	return make_uniq<RegexpMatchesBindData>(options, constant_string, constant_pattern, range_min, range_max,
	                                        range_success);
}

unique_ptr<FunctionData> RegexpMatchesBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	// pattern is the second argument. If its constant, we can already prepare the pattern and store it for later.
	D_ASSERT(arguments.size() == 2 || arguments.size() == 3);
	RE2::Options options;
	options.set_log_errors(false);
	if (arguments.size() == 3) {
		ParseRegexOptions(context, *arguments[2], options);
	}

	string constant_string;
	bool constant_pattern;
	constant_pattern = TryParseConstantPattern(context, *arguments[1], constant_string);
	return make_uniq<RegexpMatchesBindData>(options, std::move(constant_string), constant_pattern);
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

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RegexpMatchesBindData>();

	if (info.constant_pattern) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
		UnaryExecutor::Execute<string_t, bool>(strings, result, args.size(), [&](string_t input) {
			return OP::Operation(CreateStringPiece(input), lstate.constant_pattern);
		});
	} else {
		BinaryExecutor::Execute<string_t, string_t, bool>(strings, patterns, result, args.size(),
		                                                  [&](string_t input, string_t pattern) {
			                                                  RE2 re(CreateStringPiece(pattern), info.options);
			                                                  if (!re.ok()) {
				                                                  throw InvalidInputException(re.error());
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
    : RegexpBaseBindData(options, std::move(constant_string_p), constant_pattern), global_replace(global_replace) {
}

unique_ptr<FunctionData> RegexpReplaceBindData::Copy() const {
	auto copy = make_uniq<RegexpReplaceBindData>(options, constant_string, constant_pattern, global_replace);
	return std::move(copy);
}

bool RegexpReplaceBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<RegexpReplaceBindData>();
	return RegexpBaseBindData::Equals(other) && global_replace == other.global_replace;
}

static unique_ptr<FunctionData> RegexReplaceBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	auto data = make_uniq<RegexpReplaceBindData>();

	data->constant_pattern = TryParseConstantPattern(context, *arguments[1], data->constant_string);
	if (arguments.size() == 4) {
		ParseRegexOptions(context, *arguments[3], data->options, &data->global_replace);
	}
	data->options.set_log_errors(false);
	return std::move(data);
}

static void RegexReplaceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<RegexpReplaceBindData>();

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	auto &replaces = args.data[2];

	if (info.constant_pattern) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
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
    : RegexpBaseBindData(options, std::move(constant_string_p), constant_pattern),
      group_string(std::move(group_string_p)), rewrite(group_string) {
}

unique_ptr<FunctionData> RegexpExtractBindData::Copy() const {
	return make_uniq<RegexpExtractBindData>(options, constant_string, constant_pattern, group_string);
}

bool RegexpExtractBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<RegexpExtractBindData>();
	return RegexpBaseBindData::Equals(other) && group_string == other.group_string;
}

static void RegexExtractFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<RegexpExtractBindData>();

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	if (info.constant_pattern) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
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

//===--------------------------------------------------------------------===//
// Regexp Extract Struct
//===--------------------------------------------------------------------===//
static void RegexExtractStructFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();

	const auto count = args.size();
	auto &input = args.data[0];

	auto &child_entries = StructVector::GetEntries(result);
	const auto groupSize = child_entries.size();
	// Reference the 'input' StringBuffer, because we won't need to allocate new data
	// for the result, all returned strings are substrings of the originals
	for (auto &child_entry : child_entries) {
		child_entry->SetAuxiliary(input.GetAuxiliary());
	}

	vector<RE2::Arg> argv(groupSize);
	vector<RE2::Arg *> groups(groupSize);
	vector<duckdb_re2::StringPiece> ws(groupSize);
	for (size_t i = 0; i < groupSize; ++i) {
		groups[i] = &argv[i];
		argv[i] = &ws[i];
	}

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		if (ConstantVector::IsNull(input)) {
			ConstantVector::SetNull(result, true);
		} else {
			ConstantVector::SetNull(result, false);
			auto idata = ConstantVector::GetData<string_t>(input);
			auto str = CreateStringPiece(idata[0]);
			auto match = duckdb_re2::RE2::PartialMatchN(str, lstate.constant_pattern, groups.data(),
			                                            UnsafeNumericCast<int>(groups.size()));
			for (size_t col = 0; col < child_entries.size(); ++col) {
				auto &child_entry = child_entries[col];
				ConstantVector::SetNull(*child_entry, false);
				auto &extracted = ws[col];
				auto cdata = ConstantVector::GetData<string_t>(*child_entry);
				cdata[0] = string_t(extracted.data(), UnsafeNumericCast<uint32_t>(match ? extracted.size() : 0));
			}
		}
	} else {
		UnifiedVectorFormat iunified;
		input.ToUnifiedFormat(count, iunified);

		const auto &ivalidity = iunified.validity;
		auto idata = UnifiedVectorFormat::GetData<string_t>(iunified);

		// Start with a valid flat vector
		result.SetVectorType(VectorType::FLAT_VECTOR);

		// Start with valid children
		for (size_t col = 0; col < child_entries.size(); ++col) {
			auto &child_entry = child_entries[col];
			child_entry->SetVectorType(VectorType::FLAT_VECTOR);
		}

		for (idx_t i = 0; i < count; ++i) {
			const auto idx = iunified.sel->get_index(i);
			if (ivalidity.RowIsValid(idx)) {
				auto str = CreateStringPiece(idata[idx]);
				auto match = duckdb_re2::RE2::PartialMatchN(str, lstate.constant_pattern, groups.data(),
				                                            UnsafeNumericCast<int>(groups.size()));
				for (size_t col = 0; col < child_entries.size(); ++col) {
					auto &child_entry = child_entries[col];
					auto cdata = FlatVector::GetData<string_t>(*child_entry);
					auto &extracted = ws[col];
					cdata[i] = string_t(extracted.data(), UnsafeNumericCast<uint32_t>(match ? extracted.size() : 0));
				}
			} else {
				FlatVector::SetNull(result, i, true);
			}
		}
	}
}

static unique_ptr<FunctionData> RegexExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                 vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);

	duckdb_re2::RE2::Options options;

	string constant_string;
	bool constant_pattern = TryParseConstantPattern(context, *arguments[1], constant_string);

	if (arguments.size() >= 4) {
		ParseRegexOptions(context, *arguments[3], options);
	}

	string group_string = "\\0";
	if (arguments.size() >= 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw InvalidInputException("Group specification field must be a constant!");
		}
		Value group = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
		if (group.IsNull()) {
			group_string = "";
		} else if (group.type().id() == LogicalTypeId::LIST) {
			if (!constant_pattern) {
				throw BinderException("%s with LIST requires a constant pattern", bound_function.name);
			}
			auto &list_children = ListValue::GetChildren(group);
			if (list_children.empty()) {
				throw BinderException("%s requires non-empty lists of capture names", bound_function.name);
			}
			case_insensitive_set_t name_collision_set;
			child_list_t<LogicalType> struct_children;
			for (const auto &child : list_children) {
				if (child.IsNull()) {
					throw BinderException("NULL group name in %s", bound_function.name);
				}
				const auto group_name = child.ToString();
				if (name_collision_set.find(group_name) != name_collision_set.end()) {
					throw BinderException("Duplicate group name \"%s\" in %s", group_name, bound_function.name);
				}
				name_collision_set.insert(group_name);
				struct_children.emplace_back(make_pair(group_name, LogicalType::VARCHAR));
			}
			bound_function.return_type = LogicalType::STRUCT(struct_children);

			duckdb_re2::StringPiece constant_piece(constant_string.c_str(), constant_string.size());
			RE2 constant_pattern(constant_piece, options);
			if (size_t(constant_pattern.NumberOfCapturingGroups()) < list_children.size()) {
				throw BinderException("Not enough group names in %s", bound_function.name);
			}
		} else {
			auto group_idx = group.GetValue<int32_t>();
			if (group_idx < 0 || group_idx > 9) {
				throw InvalidInputException("Group index must be between 0 and 9!");
			}
			group_string = "\\" + to_string(group_idx);
		}
	}

	return make_uniq<RegexpExtractBindData>(options, std::move(constant_string), constant_pattern,
	                                        std::move(group_string));
}

ScalarFunctionSet RegexpFun::GetFunctions() {
	ScalarFunctionSet regexp_full_match("regexp_full_match");
	regexp_full_match.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                   RegexpMatchesFunction<RegexFullMatch>, RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState,
	                   LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_full_match.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                   RegexpMatchesFunction<RegexFullMatch>, RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState,
	                   LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	return (regexp_full_match);
}

ScalarFunctionSet RegexpMatchesFun::GetFunctions() {
	ScalarFunctionSet regexp_partial_match("regexp_matches");
	regexp_partial_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN, RegexpMatchesFunction<RegexPartialMatch>,
	    RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID, FunctionStability::CONSISTENT,
	    FunctionNullHandling::SPECIAL_HANDLING));
	regexp_partial_match.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	    RegexpMatchesFunction<RegexPartialMatch>, RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState,
	    LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	return (regexp_partial_match);
}

ScalarFunctionSet RegexpReplaceFun::GetFunctions() {
	ScalarFunctionSet regexp_replace("regexp_replace");
	regexp_replace.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                          LogicalType::VARCHAR, RegexReplaceFunction, RegexReplaceBind, nullptr,
	                                          nullptr, RegexInitLocalState));
	regexp_replace.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    RegexReplaceFunction, RegexReplaceBind, nullptr, nullptr, RegexInitLocalState));
	return (regexp_replace);
}

ScalarFunctionSet RegexpExtractFun::GetFunctions() {
	ScalarFunctionSet regexp_extract("regexp_extract");
	regexp_extract.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                          RegexExtractFunction, RegexExtractBind, nullptr, nullptr,
	                                          RegexInitLocalState, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                                          FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
	                                          LogicalType::VARCHAR, RegexExtractFunction, RegexExtractBind, nullptr,
	                                          nullptr, RegexInitLocalState, LogicalType::INVALID,
	                                          FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::VARCHAR,
	    RegexExtractFunction, RegexExtractBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	// REGEXP_EXTRACT(<string>, <pattern>, [<group 1 name>[, <group n name>]...])
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::VARCHAR,
	    RegexExtractStructFunction, RegexExtractBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	    FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	// REGEXP_EXTRACT(<string>, <pattern>, [<group 1 name>[, <group n name>]...], <options>)
	regexp_extract.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR), LogicalType::VARCHAR},
	    LogicalType::VARCHAR, RegexExtractStructFunction, RegexExtractBind, nullptr, nullptr, RegexInitLocalState,
	    LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	return (regexp_extract);
}

ScalarFunctionSet RegexpExtractAllFun::GetFunctions() {
	ScalarFunctionSet regexp_extract_all("regexp_extract_all");
	regexp_extract_all.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR),
	    RegexpExtractAll::Execute, RegexpExtractAll::Bind, nullptr, nullptr, RegexpExtractAll::InitLocalState,
	    LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract_all.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER}, LogicalType::LIST(LogicalType::VARCHAR),
	    RegexpExtractAll::Execute, RegexpExtractAll::Bind, nullptr, nullptr, RegexpExtractAll::InitLocalState,
	    LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	regexp_extract_all.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
	                   LogicalType::LIST(LogicalType::VARCHAR), RegexpExtractAll::Execute, RegexpExtractAll::Bind,
	                   nullptr, nullptr, RegexpExtractAll::InitLocalState, LogicalType::INVALID,
	                   FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	return (regexp_extract_all);
}

} // namespace duckdb
