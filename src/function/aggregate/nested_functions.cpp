#include "duckdb/function/aggregate/nested_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterNestedAggregates() {
	Register<ListFun>();
}

} // namespace duckdb
