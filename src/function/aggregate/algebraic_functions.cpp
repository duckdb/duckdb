#include "function/aggregate/algebraic_functions.hpp"
#include "function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	Register<Avg>();

	Register<CovarSamp>();
	Register<CovarPop>();

	Register<StdDevSamp>();
	Register<StdDevPop>();
	Register<VarPop>();
	Register<VarSamp>();
}

} // namespace duckdb

