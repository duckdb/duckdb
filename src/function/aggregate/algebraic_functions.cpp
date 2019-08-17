#include "function/aggregate/algebraic_functions.hpp"
#include "function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	AddFunction(Avg::GetFunction());

	AddFunction(CovarSamp::GetFunction());
	AddFunction(CovarPop::GetFunction());

	AddFunction(StdDevSamp::GetFunction());
	AddFunction(StdDevPop::GetFunction());
	AddFunction(VarPop::GetFunction());
	AddFunction(VarSamp::GetFunction());
}

} // namespace duckdb

