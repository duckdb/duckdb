#include "duckdb/function/scalar/string_functions.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"

using namespace std;

namespace duckdb {

struct FMTPrintf {
	template <class ctx>
	static string OP(const char *format_str, std::vector<fmt::basic_format_arg<ctx>> &format_args) {
		return fmt::vsprintf(format_str,
		                     fmt::basic_format_args<ctx>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

struct FMTFormat {
	template <class ctx>
	static string OP(const char *format_str, std::vector<fmt::basic_format_arg<ctx>> &format_args) {
		return fmt::vformat(format_str,
		                    fmt::basic_format_args<ctx>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

template <class FORMAT_FUN, class ctx>
static void printf_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &format_string = args.data[0];
	result.vector_type = VectorType::CONSTANT_VECTOR;
	for (idx_t i = 0; i < args.column_count(); i++) {
		switch (args.data[i].vector_type) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(args.data[i])) {
				// constant null! result is always NULL regardless of other input
				result.vector_type = VectorType::CONSTANT_VECTOR;
				ConstantVector::SetNull(result, true);
				return;
			}
			break;
		default:
			// FLAT VECTOR, we can directly OR the nullmask
			args.data[i].Normalify(args.size());
			result.vector_type = VectorType::FLAT_VECTOR;
			FlatVector::Nullmask(result) |= FlatVector::Nullmask(args.data[i]);
			break;
		}
	}
	idx_t count = result.vector_type == VectorType::CONSTANT_VECTOR ? 1 : args.size();

	auto format_data = FlatVector::GetData<string_t>(format_string);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t idx = 0; idx < count; idx++) {
		if (result.vector_type == VectorType::FLAT_VECTOR && FlatVector::IsNull(result, idx)) {
			// this entry is NULL: skip it
			continue;
		}

		// first fetch the format string
		auto fmt_idx = format_string.vector_type == VectorType::CONSTANT_VECTOR ? 0 : idx;
		auto format_string = format_data[fmt_idx].GetData();

		// now gather all the format arguments
		std::vector<fmt::basic_format_arg<ctx>> format_args;
		for (idx_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
			auto &col = args.data[col_idx];
			idx_t arg_idx = col.vector_type == VectorType::CONSTANT_VECTOR ? 0 : idx;
			switch (col.type) {
			case TypeId::BOOL: {
				auto arg_data = FlatVector::GetData<bool>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::INT8: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::INT16: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::INT32: {
				auto arg_data = FlatVector::GetData<int32_t>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::INT64: {
				auto arg_data = FlatVector::GetData<int64_t>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::FLOAT: {
				auto arg_data = FlatVector::GetData<float>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::DOUBLE: {
				auto arg_data = FlatVector::GetData<double>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case TypeId::VARCHAR: {
				auto arg_data = FlatVector::GetData<string_t>(col);
				format_args.emplace_back(fmt::internal::make_arg<ctx>(arg_data[arg_idx].GetData()));
				break;
			}
			default:
				throw Exception("Unsupported type for format!");
			}
		}
		// finally actually perform the format
		string dynamic_result = FORMAT_FUN::template OP<ctx>(format_string, format_args);
		result_data[idx] = StringVector::AddString(result, dynamic_result);
	}
}

void PrintfFun::RegisterFunction(BuiltinFunctions &set) {
	// fmt::printf_context, fmt::vsprintf
	ScalarFunction printf_fun =
	    ScalarFunction("printf", {SQLType::VARCHAR}, SQLType::VARCHAR, printf_function<FMTPrintf, fmt::printf_context>);
	printf_fun.varargs = SQLType::ANY;
	set.AddFunction(printf_fun);

	// fmt::format_context, fmt::vformat
	ScalarFunction format_fun =
	    ScalarFunction("format", {SQLType::VARCHAR}, SQLType::VARCHAR, printf_function<FMTFormat, fmt::format_context>);
	format_fun.varargs = SQLType::ANY;
	set.AddFunction(format_fun);
}

} // namespace duckdb
