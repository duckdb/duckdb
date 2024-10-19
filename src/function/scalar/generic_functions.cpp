#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/function/scalar/date_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterGenericFunctions() {
	Register<ConstantOrNull>();
	Register<CreateSortKeyFun>();
	Register<ErrorFun>();
	Register<ExportAggregateFunction>();
	Register<GetVariableFun>();
	Register<StrfTimeFun>();
	Register<StrpTimeFun>();
	Register<TryStrpTimeFun>();
}

} // namespace duckdb
