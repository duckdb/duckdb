#include "duckdb/function/aggregate/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedAggregates() {
	Register<ListFun>();
	Register<HistogramFun>();
}

} // namespace duckdb
