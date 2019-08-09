#include "function/aggregate_function/algebraic_functions.hpp"
#include "function/aggregate_function.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterAlgebraicAggregates() {
	AddFunction(AggregateFunction("avg", avg_get_return_type, avg_payload_size, avg_initialize, avg_update, avg_finalize));

	AddFunction(AggregateFunction("covar_samp", covar_get_return_type, covar_state_size, covar_initialize, covar_update, covarsamp_finalize));
	AddFunction(AggregateFunction("covar_pop", covar_get_return_type, covar_state_size, covar_initialize, covar_update, covarpop_finalize));

	AddFunction(AggregateFunction("stddev_samp", stddev_get_return_type, stddev_state_size, stddevsamp_initialize, stddevsamp_update, stddevsamp_finalize));
	AddFunction(AggregateFunction("stddev_pop", stddev_get_return_type, stddev_state_size, stddevsamp_initialize, stddevsamp_update, stddevpop_finalize));
	AddFunction(AggregateFunction("var_samp", stddev_get_return_type, stddev_state_size, stddevsamp_initialize, stddevsamp_update, varsamp_finalize));
	AddFunction(AggregateFunction("var_pop", stddev_get_return_type, stddev_state_size, stddevsamp_initialize, stddevsamp_update, varpop_finalize));

}

} // namespace duckdb

