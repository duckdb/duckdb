#include "duckdb/function/scalar/generic_functions.hpp"

#include <iostream>

namespace duckdb {

namespace {

static void ErrorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	for (auto entry : args.data[0].Entries<string_t>(args.size())) {
		if (!entry.value) {
			FlatVector::SetNull(result, entry.index, true);
			continue;
		}
		throw InvalidInputException(entry.value->GetString());
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
