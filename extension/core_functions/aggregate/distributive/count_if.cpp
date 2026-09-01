#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/sum_helpers.hpp"

namespace duckdb {

AggregateFunction CountIfFun::GetFunction() {
	return GetCountIfAggregateFunction();
}

} // namespace duckdb
