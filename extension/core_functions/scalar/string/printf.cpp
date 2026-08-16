#include "core_functions/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"
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

//! The type printf formats a value of the given type as
static LogicalType PrintfArgumentType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return LogicalType::BOOLEAN;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
		return LogicalType::BIGINT;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return LogicalType::UBIGINT;
	case LogicalTypeId::HUGEINT:
		return LogicalType::HUGEINT;
	case LogicalTypeId::UHUGEINT:
		return LogicalType::UHUGEINT;
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		// decimal type: add cast to double
		return LogicalType::DOUBLE;
	case LogicalTypeId::VARCHAR:
		return LogicalType::VARCHAR;
	case LogicalTypeId::UNKNOWN:
		// parameter: accept any input and rebind later
		return LogicalType::ANY;
	default:
		// all other types: add cast to string
		return LogicalType::VARCHAR;
	}
}

static unique_ptr<FunctionData> BindPrintfFunction(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() == 2);

	vector<LogicalType> value_types;
	for (auto &value : ArgumentPack::GetTypes(arguments[1]->GetReturnType())) {
		value_types.push_back(PrintfArgumentType(value.second));
	}
	bound_function.GetArguments()[1] = ArgumentPack::PositionalType(std::move(value_types));
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
static void ConvertArguments(const Vector &input, idx_t arg_idx,
                             vector<vector<duckdb_fmt::basic_format_arg<CTX>>> &result_args) {
	auto result = input.Values<T>();
	for (idx_t i = 0; i < input.size(); i++) {
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

	auto format_data = args.data[0].Values<string_t>();

	auto &values = ArgumentPack::GetInput(args.data[1]);
	for (idx_t value_idx = 0; value_idx < values.size(); value_idx++) {
		const auto &col = values[value_idx];
		const auto i = value_idx + 1;
		switch (col.GetType().id()) {
		case LogicalTypeId::BOOLEAN:
			ConvertArguments<bool>(col, i, format_args);
			break;
		case LogicalTypeId::TINYINT:
			ConvertArguments<int8_t>(col, i, format_args);
			break;
		case LogicalTypeId::SMALLINT:
			ConvertArguments<int16_t>(col, i, format_args);
			break;
		case LogicalTypeId::INTEGER:
			ConvertArguments<int32_t>(col, i, format_args);
			break;
		case LogicalTypeId::BIGINT:
			ConvertArguments<int64_t>(col, i, format_args);
			break;
		case LogicalTypeId::UBIGINT:
			ConvertArguments<uint64_t>(col, i, format_args);
			break;
		case LogicalTypeId::FLOAT:
			ConvertArguments<float>(col, i, format_args);
			break;
		case LogicalTypeId::HUGEINT:
			ConvertArguments<hugeint_t>(col, i, format_args);
			break;
		case LogicalTypeId::UHUGEINT:
			ConvertArguments<uhugeint_t>(col, i, format_args);
			break;
		case LogicalTypeId::DOUBLE:
			ConvertArguments<double>(col, i, format_args);
			break;
		case LogicalTypeId::VARCHAR:
			ConvertArguments<string_t, StringConstructArgument>(col, i, format_args);
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
		if (!entry.IsValid() || current_args.size() != values.size()) {
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

	auto sig = FunctionSignature()
		.AddParameter("format", LogicalType::VARCHAR)
		.AddVarPositionalParameter("args", LogicalType::ANY)
		.SetReturnType(LogicalType::VARCHAR);

	auto fun = ScalarFunction("printf", std::move(sig))
		.SetFunctionCallback(PrintfFunction<FMTPrintf, duckdb_fmt::printf_context>)
		.SetBindCallback(BindPrintfFunction)
		.SetFallible();

	return fun;
}

ScalarFunction FormatFun::GetFunction() {
	// duckdb_fmt::format_context, duckdb_fmt::vformat

	auto sig = FunctionSignature()
		.AddParameter("format", LogicalType::VARCHAR)
		.AddVarPositionalParameter("args", LogicalType::ANY)
		.SetReturnType(LogicalType::VARCHAR);

	auto fun = ScalarFunction("format", std::move(sig))
		.SetFunctionCallback(PrintfFunction<FMTFormat, duckdb_fmt::format_context>)
		.SetBindCallback(BindPrintfFunction)
		.SetFallible();

	return fun;
}

} // namespace duckdb
