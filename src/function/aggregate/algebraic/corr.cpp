//
// Created by holanda on 2/5/21.
//
#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate/algebraic/covar.hpp"
#include "duckdb/function/aggregate/algebraic/stddev.hpp"
#include "duckdb/function/aggregate/algebraic/corr.hpp"

namespace duckdb{
void Corr::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("corr");
	corr.AddFunction(AggregateFunction::BinaryAggregate<corr_state_t, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}
}

