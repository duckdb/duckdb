#include "duckdb/function/aggregate/holistic_functions.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterHolisticAggregates() {
	Register<QuantileFun>();
}

} // namespace duckdb
