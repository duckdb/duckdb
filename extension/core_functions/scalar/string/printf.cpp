#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/limits.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"

namespace duckdb {

struct FMTPrintf {
	template <class CTX>
	static string OP(const char *format_str, vector<duckdb_fmt::basic_format_arg<CTX>> &format_args) {
		return duckdb_fmt::vsprintf(
		    format_str, duckdb_fmt::basic_format_args<CTX>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

struct FMTFormat {
	template <class CTX>
	static string OP(const char *format_str, vector<duckdb_fmt::basic_format_arg<CTX>> &format_args) {
		return duckdb_fmt::vformat(
		    format_str, duckdb_fmt::basic_format_args<CTX>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

unique_ptr<FunctionData> BindPrintfFunction(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	for (idx_t i = 1; i < arguments.size(); i++) {
		switch (arguments[i]->return_type.id()) {
		case LogicalTypeId::BOOLEAN:
			bound_function.arguments.emplace_back(LogicalType::BOOLEAN);
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			bound_function.arguments.emplace_back(LogicalType::BIGINT);
			break;
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			bound_function.arguments.emplace_back(LogicalType::UBIGINT);
			break;
		case LogicalTypeId::HUGEINT:
			bound_function.arguments.emplace_back(LogicalType::HUGEINT);
			break;
		case LogicalTypeId::UHUGEINT:
			bound_function.arguments.emplace_back(LogicalType::UHUGEINT);
			break;
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			bound_function.arguments.emplace_back(LogicalType::DOUBLE);
			break;
		case LogicalTypeId::VARCHAR:
			bound_function.arguments.push_back(LogicalType::VARCHAR);
			break;
		case LogicalTypeId::DECIMAL:
			// decimal type: add cast to double
			bound_function.arguments.emplace_back(LogicalType::DOUBLE);
			break;
		case LogicalTypeId::UNKNOWN:
			// parameter: accept any input and rebind later
			bound_function.arguments.emplace_back(LogicalType::ANY);
			break;
		default:
			// all other types: add cast to string
			bound_function.arguments.emplace_back(LogicalType::VARCHAR);
			break;
		}
	}
	return nullptr;
}

template <class FORMAT_FUN, class CTX>
static void PrintfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &format_string = args.data[0];
	auto &result_validity = FlatVector::Validity(result);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result_validity.Initialize(args.size());
	for (idx_t i = 0; i < args.ColumnCount(); i++) {
		switch (args.data[i].GetVectorType()) {
		case VectorType::CONSTANT_VECTOR:
			if (ConstantVector::IsNull(args.data[i])) {
				// constant null! result is always NULL regardless of other input
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
				return;
			}
			break;
		default:
			// FLAT VECTOR, we can directly OR the nullmask
			args.data[i].Flatten(args.size());
			result.SetVectorType(VectorType::FLAT_VECTOR);
			result_validity.Combine(FlatVector::Validity(args.data[i]), args.size());
			break;
		}
	}
	idx_t count = result.GetVectorType() == VectorType::CONSTANT_VECTOR ? 1 : args.size();

	auto format_data = FlatVector::GetData<string_t>(format_string);
	auto result_data = FlatVector::GetData<string_t>(result);
	for (idx_t idx = 0; idx < count; idx++) {
		if (result.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::IsNull(result, idx)) {
			// this entry is NULL: skip it
			continue;
		}

		// first fetch the format string
		auto fmt_idx = format_string.GetVectorType() == VectorType::CONSTANT_VECTOR ? 0 : idx;
		auto format_string = format_data[fmt_idx].GetString();

		// now gather all the format arguments
		vector<duckdb_fmt::basic_format_arg<CTX>> format_args;
		vector<unsafe_unique_array<data_t>> string_args;

		for (idx_t col_idx = 1; col_idx < args.ColumnCount(); col_idx++) {
			auto &col = args.data[col_idx];
			idx_t arg_idx = col.GetVectorType() == VectorType::CONSTANT_VECTOR ? 0 : idx;
			switch (col.GetType().id()) {
			case LogicalTypeId::BOOLEAN: {
				auto arg_data = FlatVector::GetData<bool>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::TINYINT: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::SMALLINT: {
				auto arg_data = FlatVector::GetData<int16_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::INTEGER: {
				auto arg_data = FlatVector::GetData<int32_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::BIGINT: {
				auto arg_data = FlatVector::GetData<int64_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::UBIGINT: {
				auto arg_data = FlatVector::GetData<uint64_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::FLOAT: {
				auto arg_data = FlatVector::GetData<float>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::HUGEINT: {
				auto arg_data = FlatVector::GetData<hugeint_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::UHUGEINT: {
				auto arg_data = FlatVector::GetData<uhugeint_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::DOUBLE: {
				auto arg_data = FlatVector::GetData<double>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::VARCHAR: {
				auto arg_data = FlatVector::GetData<string_t>(col);
				auto string_view =
				    duckdb_fmt::basic_string_view<char>(arg_data[arg_idx].GetData(), arg_data[arg_idx].GetSize());
				format_args.emplace_back(duckdb_fmt::internal::make_arg<CTX>(string_view));
				break;
			}
			default:
				throw InternalException("Unexpected type for printf format");
			}
		}
		// finally actually perform the format
		string dynamic_result = FORMAT_FUN::template OP<CTX>(format_string.c_str(), format_args);
		result_data[idx] = StringVector::AddString(result, dynamic_result);
	}
}

ScalarFunction PrintfFun::GetFunction() {
	// duckdb_fmt::printf_context, duckdb_fmt::vsprintf
	ScalarFunction printf_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTPrintf, duckdb_fmt::printf_context>, BindPrintfFunction);
	printf_fun.varargs = LogicalType::ANY;
	printf_fun.SetFallible();
	return printf_fun;
}

ScalarFunction FormatFun::GetFunction() {
	// duckdb_fmt::format_context, duckdb_fmt::vformat
	ScalarFunction format_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTFormat, duckdb_fmt::format_context>, BindPrintfFunction);
	format_fun.varargs = LogicalType::ANY;
	format_fun.SetFallible();
	return format_fun;
}

} // namespace duckdb
