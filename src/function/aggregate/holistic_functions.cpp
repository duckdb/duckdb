#include "duckdb/function/aggregate/holistic_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterHolisticAggregates() {
	Register<QuantileFun>();
}

} // namespace duckdb
