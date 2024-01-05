#include "duckdb/core_functions/scalar/string_functions.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/common/local_file_system.hpp"
#include <iostream>

namespace duckdb {

static string GetSeparator(const string_t &input) {
	string option = input.GetString();

	// system's path separator
	auto fs = FileSystem::CreateLocal();
	auto system_sep = fs->PathSeparator(option);

    string separator;
	if (option == "system") {
		separator = system_sep;
	} else if (option == "forward_slash") {
		separator = "/";
	} else if (option == "backslash") {
		separator = "\\";
	} else { // both_slash (default)
		separator = "/\\";
	}
	return separator;
}

// TODO: can I somehow use the StringSplitInput from stringSplit
struct SplitInput {
	SplitInput(Vector &result_list, Vector &result_child, idx_t offset)
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

static idx_t Find(const char *input_data, idx_t input_size, const string& sep_data) {
    if (sep_data.size() == 0) {
    	return 0;
    }
    auto pos = ContainsFun::Find(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(&sep_data[0]), 1);
    // both_slash option
    if (sep_data.size() > 1) {
        auto sec_pos = ContainsFun::Find(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(&sep_data[1]), 1);
        // choose the earliest valid position
        if (sec_pos != DConstants::INVALID_INDEX && (sec_pos < pos || pos == DConstants::INVALID_INDEX)) {
            return sec_pos;
        }
    }  
    return pos;
}

static idx_t Split(string_t input, const string& sep, SplitInput &state) {
    auto input_data = input.GetData();
    auto input_size = input.GetSize();
    if(!input_size) { 
        return 0;
    }
    
    idx_t list_idx = 0; 
    while (input_size > 0) {
    	auto pos = Find(input_data, input_size, sep);
    	if (pos > input_size || pos == DConstants::INVALID_INDEX) {
		    break;
    	}
        D_ASSERT(input_size >= pos);
        state.AddSplit(input_data, pos, list_idx);
        list_idx++;
        input_data += (pos + 1);
        input_size -= (pos + 1);
    }
    state.AddSplit(input_data, input_size, list_idx);
    list_idx++;
    return list_idx;
}

static void ParsePathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(args.size(), input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);
    
    // set the separator
    string input_sep = "default";
    if (args.ColumnCount() > 1) {
        UnifiedVectorFormat sep_data;
	    args.data[1].ToUnifiedFormat(args.size(), sep_data);
        if (sep_data.validity.RowIsValid(0)) {
            input_sep = UnifiedVectorFormat::GetData<string_t>(sep_data)->GetString();
        }
    }
    const string sep = GetSeparator(input_sep);

    D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	ListVector::SetListSize(result, 0);

    // set up the list entries
    auto list_data = FlatVector::GetData<list_entry_t>(result);
    auto &child_entry = ListVector::GetEntry(result);
	auto &result_mask = FlatVector::Validity(result);
    idx_t total_splits = 0;
    for (idx_t i = 0; i < args.size(); i++) {
		auto input_idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(input_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
        SplitInput split_input(result, child_entry, total_splits);
        auto list_length = Split(inputs[input_idx], sep, split_input);
		list_data[i].length = list_length;
		list_data[i].offset = total_splits;
		total_splits += list_length;
    }
    ListVector::SetListSize(result, total_splits);
	D_ASSERT(ListVector::GetListSize(result) == total_splits);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet ParsePathFun::GetFunctions() {
    auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
    ScalarFunctionSet parse_path_func;
    ScalarFunction func({LogicalType::VARCHAR}, varchar_list_type, ParsePathFunction,
	                         nullptr, nullptr, nullptr, nullptr, LogicalType::INVALID,
	                         FunctionSideEffects::NO_SIDE_EFFECTS, FunctionNullHandling::SPECIAL_HANDLING);
    parse_path_func.AddFunction(func);
    // separator options
    func.arguments.emplace_back(LogicalType::VARCHAR);
    parse_path_func.AddFunction(func);

    return parse_path_func;
}

} // namespace duckdb
