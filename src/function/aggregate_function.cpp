#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

bool AggregateFunctionProperties::operator==(const AggregateFunctionProperties &rhs) const {
	return FunctionProperties::operator==(rhs) && order_dependent == rhs.order_dependent &&
	       distinct_dependent == rhs.distinct_dependent;
}
bool AggregateFunctionProperties::operator!=(const AggregateFunctionProperties &rhs) const {
	return !(*this == rhs);
}

bool AggregateFunctionCallbacks::operator==(const AggregateFunctionCallbacks &rhs) const {
	return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
	       combine == rhs.combine && finalize == rhs.finalize && simple_update == rhs.simple_update &&
	       window == rhs.window && window_init == rhs.window_init && window_batch == rhs.window_batch &&
	       bind == rhs.bind && destructor == rhs.destructor && statistics == rhs.statistics &&
	       serialize == rhs.serialize && deserialize == rhs.deserialize;
}

bool AggregateFunctionCallbacks::operator!=(const AggregateFunctionCallbacks &rhs) const {
	return !(*this == rhs);
}

AggregateFunctionInfo::~AggregateFunctionInfo() {
}

} // namespace duckdb
