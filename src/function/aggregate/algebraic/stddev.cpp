#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/aggregate/algebraic/stddev.hpp"
#include <cmath>

namespace duckdb {

void StdDevSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet stddev_samp("stddev_samp");
	stddev_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev_samp);
	AggregateFunctionSet stddev("stddev");
	stddev.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev);
}

void StdDevPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet stddev_pop("stddev_pop");
	stddev_pop.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, STDDevPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(stddev_pop);
}

void VarPopFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_pop("var_pop");
	var_pop.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarPopOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_pop);
}

void VarSampFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_samp("var_samp");
	var_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_samp);
}
void VarianceFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet var_samp("variance");
	var_samp.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, VarSampOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(var_samp);
}

void StandardErrorOfTheMeanFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet sem("sem");
	sem.AddFunction(AggregateFunction::UnaryAggregate<StddevState, double, double, StandardErrorOfTheMeanOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(sem);
}

} // namespace duckdb
