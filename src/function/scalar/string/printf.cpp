#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/common/limits.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"

using namespace std;

namespace duckdb {

struct FMTPrintf {
	template <class ctx>
	static string OP(const char *format_str, std::vector<duckdb_fmt::basic_format_arg<ctx>> &format_args) {
		return duckdb_fmt::vsprintf(
		    format_str, duckdb_fmt::basic_format_args<ctx>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

struct FMTFormat {
	template <class ctx>
	static string OP(const char *format_str, std::vector<duckdb_fmt::basic_format_arg<ctx>> &format_args) {
		return duckdb_fmt::vformat(
		    format_str, duckdb_fmt::basic_format_args<ctx>(format_args.data(), static_cast<int>(format_args.size())));
	}
};

unique_ptr<FunctionData> bind_printf_function(BoundFunctionExpression &expr, ClientContext &context) {
	for(idx_t i = 1; i < expr.children.size(); i++) {
		switch(expr.children[i]->return_type.id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
		case LogicalTypeId::VARCHAR:
			// these types are natively supported
			break;
		case LogicalTypeId::DECIMAL:
			// decimal type: add cast to double
			expr.children[i] = BoundCastExpression::AddCastToType(move(expr.children[i]), LogicalType::DOUBLE);
			break;
		default:
			// all other types: add cast to string
			expr.children[i] = BoundCastExpression::AddCastToType(move(expr.children[i]), LogicalType::VARCHAR);
			break;
		}
	}
	return nullptr;
}

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
		std::vector<duckdb_fmt::basic_format_arg<ctx>> format_args;
		for (idx_t col_idx = 1; col_idx < args.column_count(); col_idx++) {
			auto &col = args.data[col_idx];
			idx_t arg_idx = col.vector_type == VectorType::CONSTANT_VECTOR ? 0 : idx;
			switch (col.type.id()) {
			case LogicalTypeId::BOOLEAN: {
				auto arg_data = FlatVector::GetData<bool>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::TINYINT: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::SMALLINT: {
				auto arg_data = FlatVector::GetData<int8_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::INTEGER: {
				auto arg_data = FlatVector::GetData<int32_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::BIGINT: {
				auto arg_data = FlatVector::GetData<int64_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::FLOAT: {
				auto arg_data = FlatVector::GetData<float>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::DOUBLE: {
				auto arg_data = FlatVector::GetData<double>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx]));
				break;
			}
			case LogicalTypeId::VARCHAR: {
				auto arg_data = FlatVector::GetData<string_t>(col);
				format_args.emplace_back(duckdb_fmt::internal::make_arg<ctx>(arg_data[arg_idx].GetData()));
				break;
			}
			default:
				throw InvalidInputException("Unsupported type for format: \"%s\"!", col.type.ToString());
			}
		}
		// finally actually perform the format
		string dynamic_result = FORMAT_FUN::template OP<ctx>(format_string, format_args);
		result_data[idx] = StringVector::AddString(result, dynamic_result);
	}
}

void PrintfFun::RegisterFunction(BuiltinFunctions &set) {
	// duckdb_fmt::printf_context, duckdb_fmt::vsprintf
	ScalarFunction printf_fun = ScalarFunction("printf", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                           printf_function<FMTPrintf, duckdb_fmt::printf_context>,
											   false, bind_printf_function);
	printf_fun.varargs = LogicalType::ANY;
	set.AddFunction(printf_fun);

	// duckdb_fmt::format_context, duckdb_fmt::vformat
	ScalarFunction format_fun = ScalarFunction("format", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                           printf_function<FMTFormat, duckdb_fmt::format_context>,
											   false, bind_printf_function);
	format_fun.varargs = LogicalType::ANY;
	set.AddFunction(format_fun);
}

} // namespace duckdb
