#include "duckdb/core_functions/aggregate/algebraic_functions.hpp"
#include "duckdb/core_functions/aggregate/algebraic/covar.hpp"
#include "duckdb/core_functions/aggregate/algebraic/stddev.hpp"
#include "duckdb/core_functions/aggregate/algebraic/corr.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

AggregateFunction CorrFun::GetFunction() {
	return AggregateFunction::BinaryAggregate<CorrState, double, double, double, CorrOperation>(
	    LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE);
}
} // namespace duckdb
