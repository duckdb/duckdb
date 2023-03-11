#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "re2/re2.h"

namespace duckdb {

using regexp_util::CreateStringPiece;
using regexp_util::Extract;
using regexp_util::ParseRegexOptions;
using regexp_util::TryParseConstantPattern;

unique_ptr<FunctionLocalState>
RegexpExtractAll::InitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &info = (RegexpBaseBindData &)*bind_data;
	if (info.constant_pattern) {
		return make_unique<RegexLocalState>(info, true);
	}
	return nullptr;
}

// Forwards startpos automatically
bool ExtractAll(duckdb_re2::StringPiece &input, duckdb_re2::RE2 &pattern, idx_t *startpos,
                duckdb_re2::StringPiece *groups, int ngroups) {

	D_ASSERT(pattern.ok());
	D_ASSERT(pattern.NumberOfCapturingGroups() == ngroups);

	if (!pattern.Match(input, *startpos, input.size(), pattern.Anchored(), groups, ngroups + 1)) {
		return false;
	}
	idx_t consumed = static_cast<size_t>(groups[0].end() - (input.begin() + *startpos));
	if (!consumed) {
		// Empty match found, have to manually forward the input
		// to avoid an infinite loop
		// FIXME: support unicode characters
		consumed++;
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
	for (idx_t iteration = 0; ExtractAll(input, pattern, &startpos, args.group_buffer, args.size); iteration++) {
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
			list_content[child_idx] = string_t(string.GetDataUnsafe(), 0);
			if (match_group.begin() == nullptr) {
				// This group is optional
				child_validity.SetInvalid(child_idx);
			}
		} else {
			// Every group is a substring of the original, we can find out the offset using the pointer
			// the 'match_group' address is guaranteed to be bigger than that of the source
			D_ASSERT((const char *)match_group.begin() >= string.GetDataUnsafe());
			idx_t offset = match_group.begin() - string.GetDataUnsafe();
			list_content[child_idx] = string_t(string.GetDataUnsafe() + offset, match_group.size());
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

int32_t GetGroupIndex(DataChunk &args, idx_t row) {
	if (args.ColumnCount() < 3) {
		return 0;
	}
	UnifiedVectorFormat format;
	args.data[2].ToUnifiedFormat(args.size(), format);
	return ((int32_t *)format.data)[format.sel->get_index(row)];
}

void RegexpExtractAll::Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (RegexpBaseBindData &)*func_expr.bind_info;

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
	if (info.constant_pattern) {
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);

		auto group_count = lstate.constant_pattern.NumberOfCapturingGroups();
		if (group_count == -1) {
			throw InvalidInputException("Pattern failed to parse, error: '%s'", lstate.constant_pattern.error());
		}

		for (idx_t row = 0; row < tuple_count; row++) {
			auto idx = strings_data.sel->get_index(row);
			auto string = ((string_t *)strings_data.data)[idx];

			auto &string_validity = strings_data.validity;
			if (!string_validity.RowIsValid(idx)) {
				// String is NULL, result is also NULL
				auto result_data = FlatVector::GetData<list_entry_t>(result);
				auto &result_validity = FlatVector::Validity(result);
				result_data[row].length = 0;
				result_data[row].offset = ListVector::GetListSize(result);
				result_validity.SetInvalid(row);
				continue;
			}
			int32_t group_index = GetGroupIndex(args, row);
			// Get the groups
			ExtractSingleTuple(string, lstate.constant_pattern, group_index, lstate.group_buffer, result, row);
		}
	} else {
		RegexStringPieceArgs string_pieces;
		for (idx_t row = 0; row < tuple_count; row++) {
			auto pattern_idx = pattern_data.sel->get_index(row);
			auto &validity = pattern_data.validity;
			if (!validity.RowIsValid(pattern_idx)) {
				// Pattern is NULL, result is also NULL
				auto result_data = FlatVector::GetData<list_entry_t>(result);
				auto &result_validity = FlatVector::Validity(result);
				result_data[row].length = 0;
				result_data[row].offset = ListVector::GetListSize(result);
				result_validity.SetInvalid(row);
				continue;
			}

			auto string_idx = strings_data.sel->get_index(row);
			auto &string = ((string_t *)strings_data.data)[string_idx];

			auto &pattern_p = ((string_t *)pattern_data.data)[pattern_idx];
			auto pattern_strpiece = CreateStringPiece(pattern_p);
			duckdb_re2::RE2 re(pattern_strpiece, info.options);

			auto group_count_p = re.NumberOfCapturingGroups();
			if (group_count_p == -1) {
				throw InvalidInputException("Pattern failed to parse, error: '%s'", re.error());
			}
			string_pieces.SetSize(group_count_p);

			idx_t group_index = GetGroupIndex(args, row);
			ExtractSingleTuple(string, re, group_index, string_pieces, result, row);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
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
	return make_unique<RegexpExtractBindData>(options, std::move(constant_string), constant_pattern, "");
}

} // namespace duckdb
