#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/planner/expression/bound_argument_pack.hpp"

#include <string.h>

namespace duckdb {

static void ConcatWSFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	auto sep_data = args.data[0].Values<string_t>();
	vector<VectorIterator<string_t>> iterators;
	iterators.emplace_back(args.data[1].Values<string_t>());
	for (auto &value : StructVector::GetEntries(args.data[2])) {
		iterators.emplace_back(value.Values<string_t>());
	}
	const auto value_count = iterators.size();

	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t r = 0; r < count; r++) {
		auto sep_entry = sep_data[r];
		if (!sep_entry.IsValid()) {
			result_data.WriteNull();
			continue;
		}
		auto sep = sep_entry.GetValue();
		auto sep_ptr = sep.GetData();
		auto sep_size = sep.GetSize();

		// first figure out the length of the result string
		idx_t result_length = 0;
		bool has_result = false;
		for (idx_t col_idx = 0; col_idx < value_count; col_idx++) {
			auto &idata = iterators[col_idx];
			auto input = idata[r];
			if (!input.IsValid()) {
				continue;
			}
			if (has_result) {
				result_length += sep.GetSize();
			}
			result_length += input.GetValue().GetSize();
			has_result = true;
		}
		auto &result_str = result_data.WriteEmptyString(result_length);
		auto result_ptr = result_str.GetDataWriteable();
		// now write the result string
		result_length = 0;
		has_result = false;
		for (idx_t col_idx = 0; col_idx < value_count; col_idx++) {
			auto &idata = iterators[col_idx];
			auto input = idata[r];
			if (!input.IsValid()) {
				continue;
			}
			if (has_result) {
				memcpy(result_ptr + result_length, sep_ptr, sep_size);
				result_length += sep.GetSize();
			}
			auto input_str = input.GetValue();
			memcpy(result_ptr + result_length, input_str.GetData(), input_str.GetSize());
			result_length += input.GetValue().GetSize();
			has_result = true;
		}
		result_str.Finalize();
	}
}

static unique_ptr<FunctionData> BindConcatWSFunction(BindScalarFunctionInput &input) {
	auto &bound_function = input.GetBoundFunction();
	auto &arguments = bound_function.GetArguments();
	D_ASSERT(arguments.size() == 3);
	// every value is concatenated as a string, including the ones collected by "*values"
	arguments[0] = LogicalType::VARCHAR;
	arguments[1] = LogicalType::VARCHAR;
	const auto packed_count = StructType::GetChildCount(arguments[2]);
	arguments[2] = ArgumentPack::PositionalType(vector<LogicalType>(packed_count, LogicalType::VARCHAR));
	return nullptr;
}

ScalarFunction ConcatWsFun::GetFunction() {
	// concat_ws functions similarly to the concat function, except the result is NULL if the separator is NULL
	// if the separator is not NULL, however, NULL values are counted as empty string
	// there is one separate rule: there are no separators added between NULL values,
	// so the NULL value and empty string are different!
	// e.g.:
	// concat_ws(',', NULL, NULL) = ""
	// concat_ws(',', '', '') = ","

	FunctionSignature signature;
	signature.AddParameter("separator", LogicalType::VARCHAR);
	signature.AddParameter("arg", LogicalType::ANY);
	signature.AddVarPositionalParameter("args", LogicalType::ANY);
	signature.SetReturnType(LogicalType::VARCHAR);

	ScalarFunction concat_ws("concat_ws", std::move(signature), ConcatWSFunction);
	concat_ws.SetBindCallback(BindConcatWSFunction);
	concat_ws.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return ScalarFunction(concat_ws);
}

} // namespace duckdb
