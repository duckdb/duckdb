#include "duckdb/function/aggregate_function.hpp"

#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

AggregateInputData::AggregateInputData(const BoundAggregateExpression &expr, ArenaAllocator &allocator_p,
                                       AggregateCombineType combine_type_p)
    : AggregateInputData(expr.Function(), expr.BindInfo().get(), allocator_p, combine_type_p) {
}

AggregateInputData::AggregateInputData(const AggregateObject &aggr, ArenaAllocator &allocator_p,
                                       AggregateCombineType combine_type_p)
    : AggregateInputData(aggr.function, aggr.bind_data_wrapper ? aggr.bind_data_wrapper->function_data.get() : nullptr,
                         allocator_p, combine_type_p) {
}

AggregateFinalizeInputData::AggregateFinalizeInputData(const BoundAggregateFunction &function_p,
                                                       optional_ptr<FunctionData> bind_data_p,
                                                       ArenaAllocator &allocator_p,
                                                       optional_ptr<FunctionLocalState> local_state_p)
    : AggregateInputData(function_p, bind_data_p, allocator_p), local_state(local_state_p) {
	InitializeLocalState();
}

AggregateFinalizeInputData::AggregateFinalizeInputData(const BoundAggregateExpression &expr,
                                                       ArenaAllocator &allocator_p,
                                                       optional_ptr<FunctionLocalState> local_state_p)
    : AggregateInputData(expr, allocator_p), local_state(local_state_p) {
	InitializeLocalState();
}

AggregateFinalizeInputData::AggregateFinalizeInputData(const AggregateObject &aggr, ArenaAllocator &allocator_p,
                                                       optional_ptr<FunctionLocalState> local_state_p)
    : AggregateInputData(aggr, allocator_p), local_state(local_state_p) {
	InitializeLocalState();
}

void AggregateFinalizeInputData::InitializeLocalState() {
	if (local_state) {
		// the caller passed in an externally-owned local state
		return;
	}
	auto &callbacks = function.GetCallbacks();
	if (callbacks.HasInitLocalStateFinalizeCallback()) {
		owned_state = callbacks.GetInitLocalStateFinalizeCallback()(function, bind_data);
		local_state = owned_state.get();
	}
}

bool AggregateFunctionProperties::operator==(const AggregateFunctionProperties &rhs) const {
	return FunctionProperties::operator==(rhs) && order_dependent == rhs.order_dependent &&
	       distinct_dependent == rhs.distinct_dependent;
}
bool AggregateFunctionProperties::operator!=(const AggregateFunctionProperties &rhs) const {
	return !(*this == rhs);
}

bool AggregateFunctionCallbacks::operator==(const AggregateFunctionCallbacks &rhs) const {
	return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
	       combine == rhs.combine && finalize == rhs.finalize &&
	       init_local_state_finalize == rhs.init_local_state_finalize && cluster_update == rhs.cluster_update &&
	       window == rhs.window && window_init == rhs.window_init && window_batch == rhs.window_batch &&
	       bind == rhs.bind && destructor == rhs.destructor && statistics == rhs.statistics &&
	       serialize == rhs.serialize && deserialize == rhs.deserialize && get_state_type == rhs.get_state_type &&
	       export_aggregate_state == rhs.export_aggregate_state && import_aggregate_state == rhs.import_aggregate_state;
}

bool AggregateFunctionCallbacks::operator!=(const AggregateFunctionCallbacks &rhs) const {
	return !(*this == rhs);
}

AggregateFunctionInfo::~AggregateFunctionInfo() {
}

unique_ptr<BoundAggregateExpression> AggregateFunction::Bind(ClientContext &context,
                                                             vector<unique_ptr<Expression>> arguments) const {
	FunctionBinder func_binder(context);
	return func_binder.BindAggregateFunction(*this, std::move(arguments));
}

BoundAggregateFunction::BoundAggregateFunction(const AggregateFunction &function) {
	name = function.name;
	schema_name = function.schema_name;
	catalog_name = function.catalog_name;
	extra_info = function.extra_info;
	return_type = function.GetReturnType();
	properties = function.GetProperties();
	callbacks = function.GetCallbacks();
	function_info = function.GetFunctionInfo();

	// Try to default bind the function, to fill in any missing information in the BoundScalarFunction (e.g. from the
	// "bind" callback)
	for (auto &param : function.GetSignature().GetParameters()) {
		arguments.push_back(param.GetType());
	}
}

bool BoundAggregateFunction::operator==(const BoundAggregateFunction &rhs) const {
	return callbacks == rhs.callbacks && properties == rhs.properties && arguments == rhs.arguments &&
	       return_type == rhs.return_type;
}
bool BoundAggregateFunction::operator!=(const BoundAggregateFunction &rhs) const {
	return !(*this == rhs);
}

void BoundAggregateFunction::ReplaceImplementation(const AggregateFunction &function) {
	this->name = function.name;
	this->schema_name = function.schema_name;
	this->catalog_name = function.catalog_name;
	this->return_type = function.GetReturnType();
	this->properties = function.GetProperties();
	this->callbacks = function.GetCallbacks();
	this->function_info = function.GetFunctionInfo();

	// Try to default bind the function, to fill in any missing information in the BoundScalarFunction (e.g. from the
	// "bind" callback)
	arguments.clear();
	for (auto &param : function.GetSignature().GetParameters()) {
		arguments.push_back(param.GetType());
	}
}

} // namespace duckdb
