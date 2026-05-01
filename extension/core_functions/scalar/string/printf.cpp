#include "core_functions/scalar/string_functions.hpp"
#include "fmt/format.h"
#include "fmt/printf.h"
#include "duckdb/common/enums/vector_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression.hpp"
#include "fmt/core.h"

namespace duckdb {
class ClientContext;
struct ExpressionState;

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

static unique_ptr<FunctionData> BindPrintfFunction(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	for (idx_t i = 1; i < arguments.size(); i++) {
		switch (arguments[i]->GetReturnType().id()) {
		case LogicalTypeId::BOOLEAN:
			bound_function.GetArguments().emplace_back(LogicalType::BOOLEAN);
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
			bound_function.GetArguments().emplace_back(LogicalType::BIGINT);
			break;
		case LogicalTypeId::UTINYINT:
		case LogicalTypeId::USMALLINT:
		case LogicalTypeId::UINTEGER:
		case LogicalTypeId::UBIGINT:
			bound_function.GetArguments().emplace_back(LogicalType::UBIGINT);
			break;
		case LogicalTypeId::HUGEINT:
			bound_function.GetArguments().emplace_back(LogicalType::HUGEINT);
			break;
		case LogicalTypeId::UHUGEINT:
			bound_function.GetArguments().emplace_back(LogicalType::UHUGEINT);
			break;
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			bound_function.GetArguments().emplace_back(LogicalType::DOUBLE);
			break;
		case LogicalTypeId::VARCHAR:
			bound_function.GetArguments().push_back(LogicalType::VARCHAR);
			break;
		case LogicalTypeId::DECIMAL:
			// decimal type: add cast to double
			bound_function.GetArguments().emplace_back(LogicalType::DOUBLE);
			break;
		case LogicalTypeId::UNKNOWN:
			// parameter: accept any input and rebind later
			bound_function.GetArguments().emplace_back(LogicalType::ANY);
			break;
		default:
			// all other types: add cast to string
			bound_function.GetArguments().emplace_back(LogicalType::VARCHAR);
			break;
		}
	}
	return nullptr;
}

struct StandardConstructArgument {
	template <class T, class CTX>
	static void ConstructArgument(const T &input, vector<duckdb_fmt::basic_format_arg<CTX>> &result) {
		result.emplace_back(duckdb_fmt::internal::make_arg<CTX>(input));
	}
};

struct StringConstructArgument {
	template <class T, class CTX>
	static void ConstructArgument(const T &input, vector<duckdb_fmt::basic_format_arg<CTX>> &result) {
		auto string_view = duckdb_fmt::basic_string_view<char>(input.GetData(), input.GetSize());
		result.emplace_back(duckdb_fmt::internal::make_arg<CTX>(string_view));
	}
};

template <class T, class OP = StandardConstructArgument, class CTX>
static void ConvertArguments(const Vector &input, idx_t count, idx_t arg_idx,
                             vector<vector<duckdb_fmt::basic_format_arg<CTX>>> &result_args) {
	auto result = input.Values<T>(count);
	for (idx_t i = 0; i < count; i++) {
		auto &args = result_args[i];
		if (args.size() != arg_idx - 1) {
			// this entry has a NULL as one of the parameters
			continue;
		}
		auto entry = result[i];
		if (!entry.IsValid()) {
			args.clear();
			continue;
		}
		OP::ConstructArgument(entry.GetValue(), args);
	}
}

template <class FORMAT_FUN, class CTX>
static void PrintfFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	idx_t count = args.size();

	// convert all format arguments
	vector<vector<duckdb_fmt::basic_format_arg<CTX>>> format_args;
	format_args.resize(count);

	auto format_data = args.data[0].Values<string_t>(count);

	for (idx_t i = 1; i < args.ColumnCount(); i++) {
		auto &col = args.data[i];
		switch (col.GetType().id()) {
		case LogicalTypeId::BOOLEAN:
			ConvertArguments<bool>(col, count, i, format_args);
			break;
		case LogicalTypeId::TINYINT:
			ConvertArguments<int8_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::SMALLINT:
			ConvertArguments<int16_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::INTEGER:
			ConvertArguments<int32_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::BIGINT:
			ConvertArguments<int64_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::UBIGINT:
			ConvertArguments<uint64_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::FLOAT:
			ConvertArguments<float>(col, count, i, format_args);
			break;
		case LogicalTypeId::HUGEINT:
			ConvertArguments<hugeint_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::UHUGEINT:
			ConvertArguments<uhugeint_t>(col, count, i, format_args);
			break;
		case LogicalTypeId::DOUBLE:
			ConvertArguments<double>(col, count, i, format_args);
			break;
		case LogicalTypeId::VARCHAR:
			ConvertArguments<string_t, StringConstructArgument>(col, count, i, format_args);
			break;
		default:
			throw InternalException("Unexpected type for printf format");
		}
	}

	// now perform the actual formatting
	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t idx = 0; idx < count; idx++) {
		auto entry = format_data[idx];
		auto &current_args = format_args[idx];
		if (!entry.IsValid() || current_args.size() != args.ColumnCount() - 1) {
			// either format string or one of the input arguments is NULL
			result_data.WriteNull();
			continue;
		}

		auto format_string = entry.GetValue().GetString();

		// finally actually perform the format
		string dynamic_result = FORMAT_FUN::template OP<CTX>(format_string.c_str(), current_args);
		result_data.WriteValue(dynamic_result);
	}
}

ScalarFunction PrintfFun::GetFunction() {
	// duckdb_fmt::printf_context, duckdb_fmt::vsprintf
	ScalarFunction printf_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTPrintf, duckdb_fmt::printf_context>, BindPrintfFunction);
	printf_fun.SetVarArgs(LogicalType::ANY);
	printf_fun.SetFallible();
	return printf_fun;
}

ScalarFunction FormatFun::GetFunction() {
	// duckdb_fmt::format_context, duckdb_fmt::vformat
	ScalarFunction format_fun({LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                          PrintfFunction<FMTFormat, duckdb_fmt::format_context>, BindPrintfFunction);
	format_fun.SetVarArgs(LogicalType::ANY);
	format_fun.SetFallible();
	return format_fun;
}

} // namespace duckdb
