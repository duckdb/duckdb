#include "duckdb/function/aggregate/algebraic_functions.hpp"
#include "duckdb/function/aggregate/algebraic/covar.hpp"
#include "duckdb/function/aggregate/algebraic/stddev.hpp"
#include "duckdb/function/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
void Corr::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet corr("corr");
	corr.AddFunction(AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE));
	set.AddFunction(corr);
}
} // namespace duckdb
