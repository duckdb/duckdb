#include "duckdb/function/aggregate/nested_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterNestedAggregates() {
	Register<ListFun>();
}

} // namespace duckdb
