#include "duckdb/function/scalar/generic_functions.hpp"

#include <iostream>

namespace duckdb {

namespace {

static void ErrorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat vdata;
	args.data[0].ToUnifiedFormat(args.size(), vdata);

	auto strings = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		throw InvalidInputException(strings[idx].GetString());
	}
}

} // namespace

ScalarFunction ErrorFun::GetFunction() {
	auto fun = ScalarFunction("error", {LogicalType::VARCHAR}, LogicalType::SQLNULL, ErrorFunction);
	// Set the function with side effects to avoid the optimization.
	fun.SetVolatile();
	fun.SetFallible();
	return fun;
}

} // namespace duckdb
