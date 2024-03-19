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

static bool IsIdxValid(const idx_t &i, const idx_t &sentence_size) {
	if (i > sentence_size || i == DConstants::INVALID_INDEX) {
		return false;
	}
	return true;
}

static idx_t Find(const char *input_data, idx_t input_size, const string &sep_data) {
	if (sep_data.empty()) {
		return 0;
	}
	auto pos = ContainsFun::Find(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(&sep_data[0]), 1);
	// both_slash option
	if (sep_data.size() > 1) {
		auto sec_pos =
		    ContainsFun::Find(const_uchar_ptr_cast(input_data), input_size, const_uchar_ptr_cast(&sep_data[1]), 1);
		// choose the leftmost valid position
		if (sec_pos != DConstants::INVALID_INDEX && (sec_pos < pos || pos == DConstants::INVALID_INDEX)) {
			return sec_pos;
		}
	}
	return pos;
}

static idx_t FindLast(const char *data_ptr, idx_t input_size, const string &sep_data) {
	idx_t start = 0;
	while (input_size > 0) {
		auto pos = Find(data_ptr, input_size, sep_data);
		if (!IsIdxValid(pos, input_size)) {
			break;
		}
		start += (pos + 1);
		data_ptr += (pos + 1);
		input_size -= (pos + 1);
	}
	if (start < 1) {
		return DConstants::INVALID_INDEX;
	}
	return start - 1;
}

static idx_t SplitPath(string_t input, const string &sep, SplitInput &state) {
	auto input_data = input.GetData();
	auto input_size = input.GetSize();
	if (!input_size) {
		return 0;
	}
	idx_t list_idx = 0;
	while (input_size > 0) {
		auto pos = Find(input_data, input_size, sep);
		if (!IsIdxValid(pos, input_size)) {
			break;
		}

		D_ASSERT(input_size >= pos);
		if (pos == 0) {
			if (list_idx == 0) { // first character in path is separator
				state.AddSplit(input_data, 1, list_idx);
				list_idx++;
				if (input_size == 1) { // special case: the only character in path is a separator
					return list_idx;
				}
			} // else: separator is in the path
		} else {
			state.AddSplit(input_data, pos, list_idx);
			list_idx++;
		}
		input_data += (pos + 1);
		input_size -= (pos + 1);
	}
	if (input_size > 0) {
		state.AddSplit(input_data, input_size, list_idx);
		list_idx++;
	}
	return list_idx;
}

static void ReadOptionalArgs(DataChunk &args, Vector &sep, Vector &trim, const bool &front_trim) {
	switch (args.ColumnCount()) {
	case 1: {
		// use default values
		break;
	}
	case 2: {
		UnifiedVectorFormat sec_arg;
		args.data[1].ToUnifiedFormat(args.size(), sec_arg);
		if (sec_arg.validity.RowIsValid(0)) { // if not NULL
			switch (args.data[1].GetType().id()) {
			case LogicalTypeId::VARCHAR: {
				sep.Reinterpret(args.data[1]);
				break;
			}
			case LogicalTypeId::BOOLEAN: { // parse_path and parse_driname won't get in here
				trim.Reinterpret(args.data[1]);
				break;
			}
			default:
				throw InvalidInputException("Invalid argument type");
			}
		}
		break;
	}
	case 3: {
		if (!front_trim) {
			// set trim_extension
			UnifiedVectorFormat sec_arg;
			args.data[1].ToUnifiedFormat(args.size(), sec_arg);
			if (sec_arg.validity.RowIsValid(0)) {
				trim.Reinterpret(args.data[1]);
			}
			UnifiedVectorFormat third_arg;
			args.data[2].ToUnifiedFormat(args.size(), third_arg);
			if (third_arg.validity.RowIsValid(0)) {
				sep.Reinterpret(args.data[2]);
			}
		} else {
			throw InvalidInputException("Invalid number of arguments");
		}
		break;
	}
	default:
		throw InvalidInputException("Invalid number of arguments");
	}
}

template <bool FRONT_TRIM>
static void TrimPathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// set default values
	Vector &path = args.data[0];
	Vector separator(string_t("default"));
	Vector trim_extension(Value::BOOLEAN(false));
	ReadOptionalArgs(args, separator, trim_extension, FRONT_TRIM);

	TernaryExecutor::Execute<string_t, string_t, bool, string_t>(
	    path, separator, trim_extension, result, args.size(),
	    [&](string_t &inputs, string_t input_sep, bool trim_extension) {
		    auto data = inputs.GetData();
		    auto input_size = inputs.GetSize();
		    auto sep = GetSeparator(input_sep.GetString());

		    // find the beginning idx and the size of the result string
		    idx_t begin = 0;
		    idx_t new_size = input_size;
		    if (FRONT_TRIM) { // left trim
			    auto pos = Find(data, input_size, sep);
			    if (pos == 0) { // path starts with separator
				    pos = 1;
			    }
			    new_size = (IsIdxValid(pos, input_size)) ? pos : 0;
		    } else { // right trim
			    auto idx_last_sep = FindLast(data, input_size, sep);
			    if (IsIdxValid(idx_last_sep, input_size)) {
				    begin = idx_last_sep + 1;
			    }
			    if (trim_extension) {
				    auto idx_extension_sep = FindLast(data, input_size, ".");
				    if (begin <= idx_extension_sep && IsIdxValid(idx_extension_sep, input_size)) {
					    new_size = idx_extension_sep;
				    }
			    }
		    }
		    // copy the trimmed string
		    D_ASSERT(begin <= new_size);
		    auto target = StringVector::EmptyString(result, new_size - begin);
		    auto output = target.GetDataWriteable();
		    memcpy(output, data + begin, new_size - begin);

		    target.Finalize();
		    return target;
	    });
}

static void ParseDirpathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// set default values
	Vector &path = args.data[0];
	Vector separator(string_t("default"));
	Vector trim_extension(false);
	ReadOptionalArgs(args, separator, trim_extension, true);

	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    path, separator, result, args.size(), [&](string_t input_path, string_t input_sep) {
		    auto path = input_path.GetData();
		    auto path_size = input_path.GetSize();
		    auto sep = GetSeparator(input_sep.GetString());

		    auto last_sep = FindLast(path, path_size, sep);
		    if (last_sep == 0 && path_size == 1) {
			    last_sep = 1;
		    }
		    idx_t new_size = (IsIdxValid(last_sep, path_size)) ? last_sep : 0;

		    auto target = StringVector::EmptyString(result, new_size);
		    auto output = target.GetDataWriteable();
		    memcpy(output, path, new_size);
		    target.Finalize();
		    return StringVector::AddString(result, target);
	    });
}

static void ParsePathFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1 || args.ColumnCount() == 2);
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(args.size(), input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	// set the separator
	string input_sep = "default";
	if (args.ColumnCount() == 2) {
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
		auto list_length = SplitPath(inputs[input_idx], sep, split_input);
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

ScalarFunctionSet ParseDirnameFun::GetFunctions() {
	ScalarFunctionSet parse_dirname;
	ScalarFunction func({LogicalType::VARCHAR}, LogicalType::VARCHAR, TrimPathFunction<true>, nullptr, nullptr, nullptr,
	                    nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                    FunctionNullHandling::SPECIAL_HANDLING);
	parse_dirname.AddFunction(func);
	// separator options
	func.arguments.emplace_back(LogicalType::VARCHAR);
	parse_dirname.AddFunction(func);
	return parse_dirname;
}

ScalarFunctionSet ParseDirpathFun::GetFunctions() {
	ScalarFunctionSet parse_dirpath;
	ScalarFunction func({LogicalType::VARCHAR}, LogicalType::VARCHAR, ParseDirpathFunction, nullptr, nullptr, nullptr,
	                    nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                    FunctionNullHandling::SPECIAL_HANDLING);
	parse_dirpath.AddFunction(func);
	// separator options
	func.arguments.emplace_back(LogicalType::VARCHAR);
	parse_dirpath.AddFunction(func);
	return parse_dirpath;
}

ScalarFunctionSet ParseFilenameFun::GetFunctions() {
	ScalarFunctionSet parse_filename;
	parse_filename.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR, TrimPathFunction<false>,
	                                          nullptr, nullptr, nullptr, nullptr, LogicalType::INVALID,
	                                          FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	parse_filename.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::VARCHAR, TrimPathFunction<false>, nullptr, nullptr,
	    nullptr, nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	parse_filename.AddFunction(ScalarFunction(
	    {LogicalType::VARCHAR, LogicalType::BOOLEAN}, LogicalType::VARCHAR, TrimPathFunction<false>, nullptr, nullptr,
	    nullptr, nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT, FunctionNullHandling::SPECIAL_HANDLING));
	parse_filename.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR},
	                                          LogicalType::VARCHAR, TrimPathFunction<false>, nullptr, nullptr, nullptr,
	                                          nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                                          FunctionNullHandling::SPECIAL_HANDLING));
	return parse_filename;
}

ScalarFunctionSet ParsePathFun::GetFunctions() {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet parse_path;
	ScalarFunction func({LogicalType::VARCHAR}, varchar_list_type, ParsePathFunction, nullptr, nullptr, nullptr,
	                    nullptr, LogicalType::INVALID, FunctionStability::CONSISTENT,
	                    FunctionNullHandling::SPECIAL_HANDLING);
	parse_path.AddFunction(func);
	// separator options
	func.arguments.emplace_back(LogicalType::VARCHAR);
	parse_path.AddFunction(func);
	return parse_path;
}

} // namespace duckdb
