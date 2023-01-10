#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct StringSplitInput {
	StringSplitInput(Vector &result_list, Vector &result_child, idx_t offset)
	    : result_list(result_list), result_child(result_child), offset(offset) {
	}

	Vector &result_list;
	Vector &result_child;
	idx_t offset;

	void AddSplit(const char *split_data, idx_t split_size, idx_t list_idx) {
		auto list_entry = offset + list_idx;
		if (list_entry >= ListVector::GetListCapacity(result_list)) {
			ListVector::SetListSize(result_list, offset + list_idx);
			ListVector::Reserve(result_list, ListVector::GetListCapacity(result_list) * 2);
		}
		FlatVector::GetData<string_t>(result_child)[list_entry] =
		    StringVector::AddString(result_child, split_data, split_size);
	}
};

struct RegularStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		match_size = delim_size;
		if (delim_size == 0) {
			return 0;
		}
		return ContainsFun::Find((const unsigned char *)input_data, input_size, (const unsigned char *)delim_data,
		                         delim_size);
	}
};

struct ConstantRegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		D_ASSERT(data);
		auto regex = (duckdb_re2::RE2 *)data;
		duckdb_re2::StringPiece match;
		if (!regex->Match(duckdb_re2::StringPiece(input_data, input_size), 0, input_size, RE2::UNANCHORED, &match, 1)) {
			return DConstants::INVALID_INDEX;
		}
		match_size = match.size();
		return match.data() - input_data;
	}
};

struct RegexpStringSplit {
	static idx_t Find(const char *input_data, idx_t input_size, const char *delim_data, idx_t delim_size,
	                  idx_t &match_size, void *data) {
		duckdb_re2::RE2 regex(duckdb_re2::StringPiece(delim_data, delim_size));
		if (!regex.ok()) {
			throw InvalidInputException(regex.error());
		}
		return ConstantRegexpStringSplit::Find(input_data, input_size, delim_data, delim_size, match_size, &regex);
	}
};

struct StringSplitter {
	template <class OP>
	static idx_t Split(string_t input, string_t delim, StringSplitInput &state, void *data) {
		auto input_data = input.GetDataUnsafe();
		auto input_size = input.GetSize();
		auto delim_data = delim.GetDataUnsafe();
		auto delim_size = delim.GetSize();
		idx_t list_idx = 0;
		while (input_size > 0) {
			idx_t match_size = 0;
			auto pos = OP::Find(input_data, input_size, delim_data, delim_size, match_size, data);
			if (pos > input_size) {
				break;
			}
			if (match_size == 0 && pos == 0) {
				// special case: 0 length match and pos is 0
				// move to the next character
				for (pos++; pos < input_size; pos++) {
					if (LengthFun::IsCharacter(input_data[pos])) {
						break;
					}
				}
				if (pos == input_size) {
					break;
				}
			}
			D_ASSERT(input_size >= pos + match_size);
			state.AddSplit(input_data, pos, list_idx);

			list_idx++;
			input_data += (pos + match_size);
			input_size -= (pos + match_size);
		}
		state.AddSplit(input_data, input_size, list_idx);
		list_idx++;
		return list_idx;
	}
};

template <class OP>
static void StringSplitExecutor(DataChunk &args, ExpressionState &state, Vector &result, void *data = nullptr) {
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(args.size(), input_data);
	auto inputs = (string_t *)input_data.data;

	UnifiedVectorFormat delim_data;
	args.data[1].ToUnifiedFormat(args.size(), delim_data);
	auto delims = (string_t *)delim_data.data;

	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::SetListSize(result, 0);

	auto list_struct_data = FlatVector::GetData<list_entry_t>(result);

	// count all the splits and set up the list entries
	auto &child_entry = ListVector::GetEntry(result);
	auto &result_mask = FlatVector::Validity(result);
	idx_t total_splits = 0;
	for (idx_t i = 0; i < args.size(); i++) {
		auto input_idx = input_data.sel->get_index(i);
		auto delim_idx = delim_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(input_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		StringSplitInput split_input(result, child_entry, total_splits);
		if (!delim_data.validity.RowIsValid(delim_idx)) {
			// delim is NULL: copy the complete entry
			split_input.AddSplit(inputs[input_idx].GetDataUnsafe(), inputs[input_idx].GetSize(), 0);
			list_struct_data[i].length = 1;
			list_struct_data[i].offset = total_splits;
			total_splits++;
			continue;
		}
		auto list_length = StringSplitter::Split<OP>(inputs[input_idx], delims[delim_idx], split_input, data);
		list_struct_data[i].length = list_length;
		list_struct_data[i].offset = total_splits;
		total_splits += list_length;
	}
	ListVector::SetListSize(result, total_splits);
	D_ASSERT(ListVector::GetListSize(result) == total_splits);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void StringSplitFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	StringSplitExecutor<RegularStringSplit>(args, state, result, nullptr);
}

static void StringSplitRegexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (RegexpMatchesBindData &)*func_expr.bind_info;
	if (info.constant_pattern) {
		// fast path: pre-compiled regex
		auto &lstate = (RegexLocalState &)*ExecuteFunctionState::GetFunctionState(state);
		StringSplitExecutor<ConstantRegexpStringSplit>(args, state, result, &lstate.constant_pattern);
	} else {
		// slow path: have to re-compile regex for every row
		StringSplitExecutor<RegexpStringSplit>(args, state, result);
	}
}

void StringSplitFun::RegisterFunction(BuiltinFunctions &set) {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);

	auto regular_fun =
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitFunction);
	regular_fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	set.AddFunction({"string_split", "str_split", "string_to_array", "split"}, regular_fun);

	ScalarFunctionSet regexp_split("string_split_regex");
	ScalarFunction regex_fun({LogicalType::VARCHAR, LogicalType::VARCHAR}, varchar_list_type, StringSplitRegexFunction,
	                         RegexpMatchesBind, nullptr, nullptr, RegexInitLocalState, LogicalType::INVALID,
	                         FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING);
	regexp_split.AddFunction(regex_fun);
	// regexp options
	regex_fun.arguments.emplace_back(LogicalType::VARCHAR);
	regexp_split.AddFunction(regex_fun);
	for (auto &name : {"string_split_regex", "str_split_regex", "regexp_split_to_array"}) {
		regexp_split.name = name;
		set.AddFunction(regexp_split);
	}
}

} // namespace duckdb
