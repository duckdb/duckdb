#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	Register<AvgFun>();

	Register<CovarSampFun>();
	Register<CovarPopFun>();

	Register<StdDevSampFun>();
	Register<StdDevPopFun>();
	Register<VarPopFun>();
	Register<VarSampFun>();
	Register<VarianceFun>();

	Register<Corr>();
}

} // namespace duckdb
