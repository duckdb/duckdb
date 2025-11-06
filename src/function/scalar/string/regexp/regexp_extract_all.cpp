#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "re2/re2.h"
#include "re2/stringpiece.h"

namespace duckdb {

using regexp_util::CreateStringPiece;
using regexp_util::Extract;
using regexp_util::ParseRegexOptions;
using regexp_util::TryParseConstantPattern;

unique_ptr<FunctionLocalState>
RegexpExtractAll::InitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &info = bind_data->Cast<RegexpBaseBindData>();
	if (info.constant_pattern) {
		return make_uniq<RegexLocalState>(info, true);
	}
	return nullptr;
}

unique_ptr<FunctionLocalState> RegexpExtractAllStruct::InitLocalState(ExpressionState &state,
                                                                      const BoundFunctionExpression &expr,
                                                                      FunctionData *bind_data) {
	auto &info = bind_data->Cast<RegexpExtractAllStructBindData>();
	if (info.constant_pattern) {
		return make_uniq<RegexLocalState>(info, true);
	}
	return nullptr;
}

// Forwards startpos automatically
bool ExtractAll(duckdb_re2::StringPiece &input, duckdb_re2::RE2 &pattern, idx_t *startpos,
                duckdb_re2::StringPiece *groups, int ngroups) {
	D_ASSERT(pattern.ok());
	D_ASSERT(pattern.NumberOfCapturingGroups() == ngroups);

	if (!pattern.Match(input, *startpos, input.size(), pattern.UNANCHORED, groups, ngroups + 1)) {
		return false;
	}
	idx_t consumed = static_cast<size_t>(groups[0].end() - (input.begin() + *startpos));
	if (!consumed) {
		// Empty match: advance exactly one UTF-8 codepoint
		consumed = regexp_util::AdvanceOneUTF8Basic(input, *startpos);
	}
	*startpos += consumed;
	return true;
}

void ExtractSingleTuple(const string_t &string, duckdb_re2::RE2 &pattern, int32_t group, RegexStringPieceArgs &args,
                        Vector &result, idx_t row) {
	auto input = CreateStringPiece(string);

	auto &child_vector = ListVector::GetEntry(result);
	auto list_content = FlatVector::GetData<string_t>(child_vector);
	auto &child_validity = FlatVector::Validity(child_vector);

	auto current_list_size = ListVector::GetListSize(result);
	auto current_list_capacity = ListVector::GetListCapacity(result);

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	auto &list_entry = result_data[row];
	list_entry.offset = current_list_size;

	if (group < 0) {
		list_entry.length = 0;
		return;
	}
	// If the requested group index is out of bounds
	// we want to throw only if there is a match
	bool throw_on_group_found = (idx_t)group > args.size;

	idx_t startpos = 0;
	for (idx_t iteration = 0;
	     ExtractAll(input, pattern, &startpos, args.group_buffer, UnsafeNumericCast<int>(args.size)); iteration++) {
		if (!iteration && throw_on_group_found) {
			throw InvalidInputException("Pattern has %d groups. Cannot access group %d", args.size, group);
		}

		// Make sure we have enough room for the new entries
		if (current_list_size + 1 >= current_list_capacity) {
			ListVector::Reserve(result, current_list_capacity * 2);
			current_list_capacity = ListVector::GetListCapacity(result);
			list_content = FlatVector::GetData<string_t>(child_vector);
		}

		// Write the captured groups into the list-child vector
		auto &match_group = args.group_buffer[group];

		idx_t child_idx = current_list_size;
		if (match_group.empty()) {
			// This group was not matched
			list_content[child_idx] = string_t(string.GetData(), 0);
			if (match_group.begin() == nullptr) {
				// This group is optional
				child_validity.SetInvalid(child_idx);
			}
		} else {
			// Every group is a substring of the original, we can find out the offset using the pointer
			// the 'match_group' address is guaranteed to be bigger than that of the source
			D_ASSERT(const_char_ptr_cast(match_group.begin()) >= string.GetData());
			auto offset = UnsafeNumericCast<idx_t>(match_group.begin() - string.GetData());
			list_content[child_idx] =
			    string_t(string.GetData() + offset, UnsafeNumericCast<uint32_t>(match_group.size()));
		}
		current_list_size++;
		if (startpos > input.size()) {
			// Empty match found at the end of the string
			break;
		}
	}
	list_entry.length = current_list_size - list_entry.offset;
	ListVector::SetListSize(result, current_list_size);
}

int32_t GetGroupIndex(DataChunk &args, idx_t row, int32_t &result) {
	if (args.ColumnCount() < 3) {
		result = 0;
		return true;
	}
	UnifiedVectorFormat format;
	args.data[2].ToUnifiedFormat(args.size(), format);
	idx_t index = format.sel->get_index(row);
	if (!format.validity.RowIsValid(index)) {
		return false;
	}
	result = UnifiedVectorFormat::GetData<int32_t>(format)[index];
	return true;
}

duckdb_re2::RE2 &GetPattern(const RegexpBaseBindData &info, ExpressionState &state,
                            unique_ptr<duckdb_re2::RE2> &pattern_p) {
	if (info.constant_pattern) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
		return lstate.constant_pattern;
	}
	D_ASSERT(pattern_p);
	return *pattern_p;
}

RegexStringPieceArgs &GetGroupsBuffer(const RegexpBaseBindData &info, ExpressionState &state,
                                      unique_ptr<RegexStringPieceArgs> &groups_p) {
	if (info.constant_pattern) {
		auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();
		return lstate.group_buffer;
	}
	D_ASSERT(groups_p);
	return *groups_p;
}

void RegexpExtractAll::Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<RegexpBaseBindData>();

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &output_child = ListVector::GetEntry(result);

	UnifiedVectorFormat strings_data;
	strings.ToUnifiedFormat(args.size(), strings_data);

	UnifiedVectorFormat pattern_data;
	patterns.ToUnifiedFormat(args.size(), pattern_data);

	ListVector::Reserve(result, STANDARD_VECTOR_SIZE);
	// Reference the 'strings' StringBuffer, because we won't need to allocate new data
	// for the result, all returned strings are substrings of the originals
	output_child.SetAuxiliary(strings.GetAuxiliary());

	// Avoid doing extra work if all the inputs are constant
	idx_t tuple_count = args.AllConstant() ? 1 : args.size();

	unique_ptr<RegexStringPieceArgs> non_const_args;
	unique_ptr<duckdb_re2::RE2> stored_re;
	if (!info.constant_pattern) {
		non_const_args = make_uniq<RegexStringPieceArgs>();
	} else {
		// Verify that the constant pattern is valid
		auto &re = GetPattern(info, state, stored_re);
		auto group_count_p = re.NumberOfCapturingGroups();
		if (group_count_p == -1) {
			throw InvalidInputException("Pattern failed to parse, error: '%s'", re.error());
		}
	}

	for (idx_t row = 0; row < tuple_count; row++) {
		bool pattern_valid = true;
		if (!info.constant_pattern) {
			// Check if the pattern is NULL or not,
			// and compile the pattern if it's not constant
			auto pattern_idx = pattern_data.sel->get_index(row);
			if (!pattern_data.validity.RowIsValid(pattern_idx)) {
				pattern_valid = false;
			} else {
				auto &pattern_p = UnifiedVectorFormat::GetData<string_t>(pattern_data)[pattern_idx];
				auto pattern_strpiece = CreateStringPiece(pattern_p);
				stored_re = make_uniq<duckdb_re2::RE2>(pattern_strpiece, info.options);

				// Increase the size of the args buffer if needed
				auto group_count_p = stored_re->NumberOfCapturingGroups();
				if (group_count_p == -1) {
					throw InvalidInputException("Pattern failed to parse, error: '%s'", stored_re->error());
				}
				non_const_args->SetSize(UnsafeNumericCast<idx_t>(group_count_p));
			}
		}

		auto string_idx = strings_data.sel->get_index(row);
		int32_t group_index;
		if (!pattern_valid || !strings_data.validity.RowIsValid(string_idx) || !GetGroupIndex(args, row, group_index)) {
			// If something is NULL, the result is NULL
			// FIXME: do we even need 'SPECIAL_HANDLING'?
			auto result_data = FlatVector::GetData<list_entry_t>(result);
			auto &result_validity = FlatVector::Validity(result);
			result_data[row].length = 0;
			result_data[row].offset = ListVector::GetListSize(result);
			result_validity.SetInvalid(row);
			continue;
		}

		auto &re = GetPattern(info, state, stored_re);
		auto &groups = GetGroupsBuffer(info, state, non_const_args);
		auto &string = UnifiedVectorFormat::GetData<string_t>(strings_data)[string_idx];
		ExtractSingleTuple(string, re, group_index, groups, result, row);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static inline bool ExtractAllStruct(duckdb_re2::StringPiece &input, duckdb_re2::RE2 &re, idx_t &startpos,
                                    duckdb_re2::StringPiece *groups, int provided_groups) {
	D_ASSERT(re.ok());
	if (!re.Match(input, startpos, input.size(), re.UNANCHORED, groups, provided_groups + 1)) {
		return false;
	}
	idx_t consumed = static_cast<idx_t>(groups[0].end() - (input.begin() + startpos));
	if (!consumed) {
		consumed = regexp_util::AdvanceOneUTF8Basic(input, startpos);
	}
	startpos += consumed;
	return true;
}

static void ExtractStructAllSingleTuple(const string_t &string_val, duckdb_re2::RE2 &re,
                                        vector<duckdb_re2::StringPiece> &group_spans,
                                        vector<unique_ptr<Vector>> &child_entries, Vector &result, idx_t row) {
	const idx_t group_count = child_entries.size();
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	idx_t current_list_size = ListVector::GetListSize(result);
	list_entries[row].offset = current_list_size;

	auto input_piece = CreateStringPiece(string_val);
	idx_t startpos = 0;
	for (; ExtractAllStruct(input_piece, re, startpos, group_spans.data(), UnsafeNumericCast<int>(group_count));) {
		// Ensure capacity
		if (current_list_size + 1 >= ListVector::GetListCapacity(result)) {
			ListVector::Reserve(result, ListVector::GetListCapacity(result) * 2);
		}
		// Write each selected group
		for (idx_t g = 0; g < group_count; g++) {
			auto &child_vec = *child_entries[g];
			child_vec.SetVectorType(VectorType::FLAT_VECTOR);
			auto cdata = FlatVector::GetData<string_t>(child_vec);
			auto &span = group_spans[g + 1];
			if (span.empty()) {
				if (span.begin() == nullptr) {
					// Unmatched optional group -> always NULL
					FlatVector::Validity(child_vec).SetInvalid(current_list_size);
				}
				cdata[current_list_size] = string_t(string_val.GetData(), 0);
			} else {
				auto offset = span.begin() - string_val.GetData();
				cdata[current_list_size] =
				    string_t(string_val.GetData() + offset, UnsafeNumericCast<uint32_t>(span.size()));
			}
		}
		current_list_size++;
		if (startpos > input_piece.size()) {
			break; // empty match at end
		}
	}
	list_entries[row].length = current_list_size - list_entries[row].offset;
	ListVector::SetListSize(result, current_list_size);
}

void RegexpExtractAllStruct::Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<RegexpExtractAllStructBindData>();
	// Struct multi-match variant only supports constant pattern (enforced in Bind)
	D_ASSERT(info.constant_pattern);

	// Expect arguments: string, pattern, list_of_group_names [, options]
	auto &strings = args.data[0];

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &struct_vector = ListVector::GetEntry(result);
	D_ASSERT(struct_vector.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_entries = StructVector::GetEntries(struct_vector);
	const idx_t group_count = child_entries.size();

	// Reference original string buffer for zero-copy substring assignment
	for (auto &child : child_entries) {
		child->SetAuxiliary(strings.GetAuxiliary());
		child->SetVectorType(VectorType::FLAT_VECTOR);
	}

	UnifiedVectorFormat strings_data;
	strings.ToUnifiedFormat(args.size(), strings_data);
	ListVector::Reserve(result, STANDARD_VECTOR_SIZE);
	idx_t tuple_count = args.AllConstant() ? 1 : args.size();

	auto &lstate = ExecuteFunctionState::GetFunctionState(state)->Cast<RegexLocalState>();

	auto &list_validity = FlatVector::Validity(result);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);

	vector<duckdb_re2::StringPiece> group_spans(group_count + 1);

	for (idx_t row = 0; row < tuple_count; row++) {
		auto sindex = strings_data.sel->get_index(row);
		if (!strings_data.validity.RowIsValid(sindex)) {
			list_entries[row].offset = ListVector::GetListSize(result);
			list_entries[row].length = 0;
			list_validity.SetInvalid(row);
			continue;
		}
		auto &string_val = UnifiedVectorFormat::GetData<string_t>(strings_data)[sindex];
		ExtractStructAllSingleTuple(string_val, lstate.constant_pattern, group_spans, child_entries, result, row);
	}
	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

unique_ptr<FunctionData> RegexpExtractAllStruct::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	// arguments: string, pattern, LIST<VARCHAR> group_names [, options]
	if (arguments.size() < 3) {
		throw BinderException("regexp_extract_all struct variant requires at least 3 arguments");
	}
	duckdb_re2::RE2::Options options;
	string constant_string;
	bool constant_pattern = TryParseConstantPattern(context, *arguments[1], constant_string);
	if (!constant_pattern) {
		throw BinderException("%s with LIST requires a constant pattern", bound_function.name);
	}
	if (arguments.size() >= 4) {
		ParseRegexOptions(context, *arguments[3], options);
	}
	options.set_log_errors(false);
	vector<string> group_names;
	child_list_t<LogicalType> struct_children;
	regexp_util::ParseGroupNameList(context, bound_function.name, *arguments[2], constant_string, options, true,
	                                group_names, struct_children);
	bound_function.SetReturnType(LogicalType::LIST(LogicalType::STRUCT(struct_children)));
	return make_uniq<RegexpExtractAllStructBindData>(options, std::move(constant_string), constant_pattern,
	                                                 std::move(group_names));
}

unique_ptr<FunctionData> RegexpExtractAll::Bind(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);

	duckdb_re2::RE2::Options options;

	string constant_string;
	bool constant_pattern = TryParseConstantPattern(context, *arguments[1], constant_string);

	if (arguments.size() >= 4) {
		ParseRegexOptions(context, *arguments[3], options);
	}
	return make_uniq<RegexpExtractBindData>(options, std::move(constant_string), constant_pattern, "");
}

} // namespace duckdb
