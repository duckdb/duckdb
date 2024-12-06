#include "duckdb/function/scalar/generic_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterGenericFunctions() {
	Register<ConstantOrNull>();
	Register<ExportAggregateFunction>();
	Register<GetVariableFun>();
}

} // namespace duckdb
