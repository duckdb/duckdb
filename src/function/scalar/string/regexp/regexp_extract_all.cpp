#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "re2/re2.h"

namespace duckdb {

using namespace RegexpUtil;

unique_ptr<FunctionLocalState>
RegexpExtractAll::InitLocalState(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	auto &info = (RegexpBaseBindData &)*bind_data;
	if (info.constant_pattern) {
		return make_unique<RegexLocalState>(info, true);
	}
	return nullptr;
}

void RegexpExtractAll::Execute(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	const auto &info = (RegexpBaseBindData &)*func_expr.bind_info;

	auto &strings = args.data[0];
	auto &patterns = args.data[1];
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	auto &output_child = ListVector::GetEntry(result);
	auto &child_validity = FlatVector::Validity(output_child);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<list_entry_t>(result);

	UnifiedVectorFormat strings_data;
	strings.ToUnifiedFormat(args.size(), strings_data);

	UnifiedVectorFormat pattern_data;
	patterns.ToUnifiedFormat(args.size(), pattern_data);

	idx_t tuple_count = args.AllConstant() ? 1 : args.size();
	if (info.constant_pattern) {
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);

		auto group_count = lstate.constant_pattern.NumberOfCapturingGroups();
		if (group_count == -1) {
			// FIXME: probably just make the list entry NULL?
			throw InvalidInputException("Input is likely malformed, no groups were found");
		}
		const idx_t max_groups = 1 + group_count;
		// Get out pre-allocated array for the result of Match
		auto groups = lstate.group_buffer;

		// Avoid doing extra work if the strings are also constant
		output_child.Initialize(false, max_groups * tuple_count);
		// Reference the 'strings' StringBuffer, because we won't need to allocate new data
		// for the result, all returned strings are substrings of the originals
		output_child.SetAuxiliary(strings.GetAuxiliary());
		auto list_content = FlatVector::GetData<string_t>(output_child);

		for (idx_t row = 0; row < tuple_count; row++) {
			auto idx = strings_data.sel->get_index(row);
			auto string = ((string_t *)strings_data.data)[idx];
			// Get the groups
			lstate.constant_pattern.Match(CreateStringPiece(string), 0, string.GetSize(),
			                              duckdb_re2::RE2::Anchor::UNANCHORED, groups, max_groups);

			// Write them into the list-child vector
			for (idx_t group = 0; group < max_groups; group++) {
				auto &match_group = groups[group];

				// Every group is a substring of the original, we can find out the offset using the pointer
				// the 'match_group' address is guaranteed to be bigger than that of the source
				D_ASSERT((const char *)match_group.begin() <= string.GetDataUnsafe());

				idx_t child_idx = (row * max_groups) + group;
				if (match_group.begin() == nullptr || match_group.size() == 0) {
					// This group was not matched
					list_content[child_idx] = string_t(string.GetDataUnsafe(), 0);
					child_validity.SetInvalid(child_idx);
				} else {
					idx_t offset = match_group.begin() - string.GetDataUnsafe();
					list_content[child_idx] = string_t(string.GetDataUnsafe() + offset, match_group.size());
				}
			}
		}

		// Set all the list entries
		for (idx_t i = 0; i < tuple_count; i++) {
			auto &entry = result_data[i];
			entry.offset = i * max_groups;
			entry.length = max_groups;
		}

		ListVector::SetListSize(result, tuple_count * max_groups);
	} else {
		output_child.Initialize(false, STANDARD_VECTOR_SIZE);
		output_child.SetAuxiliary(strings.GetAuxiliary());
		idx_t child_capacity = STANDARD_VECTOR_SIZE;

		idx_t largest_group = 0;
		duckdb_re2::StringPiece *groups = nullptr;
		for (idx_t row = 0; row < tuple_count; row++) {
			auto pattern_idx = pattern_data.sel->get_index(row);
			auto &pattern = ((string_t *)pattern_data.data)[pattern_idx];
			duckdb_re2::RE2 re(CreateStringPiece(pattern), info.options);

			auto group_count_p = re.NumberOfCapturingGroups();
			if (group_count_p == -1) {
				// FIXME: null the list instead
				throw InvalidInputException("Pattern is malformed, no groups could be found");
			}
			idx_t group_count = 1 + group_count_p;
			if (group_count > largest_group) {
				// Reallocate the temporary group buffer if it's too small
				DeleteArray<duckdb_re2::StringPiece>(groups, largest_group);
				largest_group = group_count;
				groups = AllocateArray<duckdb_re2::StringPiece>(largest_group);
			}

			auto string_idx = strings_data.sel->get_index(row);
			auto &string = ((string_t *)strings_data.data)[string_idx];
			// Get the groups
			re.Match(CreateStringPiece(string), 0, string.GetSize(), duckdb_re2::RE2::Anchor::UNANCHORED, groups,
			         group_count);

			// Multiply the capacity if we have reached the current capacity
			if (ListVector::GetListSize(result) + group_count >= child_capacity) {
				child_capacity *= 2;
				ListVector::Reserve(result, child_capacity);
			}
			auto list_content = FlatVector::GetData<string_t>(output_child);

			// Write them into the list-child vector
			idx_t current_list_size = ListVector::GetListSize(result);
			for (idx_t group = 0; group < group_count; group++) {
				auto &match_group = groups[group];

				// Every group is a substring of the original, we can find out the offset using the pointer
				// the 'match_group' address is guaranteed to be bigger than that of the source
				D_ASSERT((const char *)match_group.begin() <= string.GetDataUnsafe());

				idx_t child_idx = current_list_size + group;
				if (match_group.begin() == nullptr || match_group.size() == 0) {
					// This group was not matched
					list_content[child_idx] = string_t(string.GetDataUnsafe(), 0);
					child_validity.SetInvalid(child_idx);
				} else {
					idx_t offset = match_group.begin() - string.GetDataUnsafe();
					list_content[child_idx] = string_t(string.GetDataUnsafe() + offset, match_group.size());
				}
			}
			auto &entry = result_data[row];
			entry.offset = current_list_size;
			entry.length = group_count;
			ListVector::SetListSize(result, current_list_size + group_count);
		}
		DeleteArray<duckdb_re2::StringPiece>(groups, largest_group);
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

	string group_string = "";
	if (arguments.size() >= 3) {
		if (arguments[2]->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!arguments[2]->IsFoldable()) {
			throw InvalidInputException("Group index field field must be a constant!");
		}
		Value group = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
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
		ParseRegexOptions(context, *arguments[3], options);
	}
	return make_unique<RegexpExtractBindData>(options, std::move(constant_string), constant_pattern,
	                                          std::move(group_string));
}

} // namespace duckdb
