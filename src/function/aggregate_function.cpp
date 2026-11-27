#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/function/aggregate_function.hpp"

#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/function/cast/cast_statistics.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

AggregateFunctionUnbindInput::AggregateFunctionUnbindInput(const BoundAggregateExpression &expression_p,
                                                           vector<unique_ptr<ParsedExpression>> children_p)
    : expression(expression_p), children(std::move(children_p)) {
}

AggregateFunctionUnbindInput::~AggregateFunctionUnbindInput() {
}

unique_ptr<BaseStatistics> AggregateFunction::PropagateInputValueStats(ClientContext &context,
                                                                       BoundAggregateExpression &expr,
                                                                       AggregateStatisticsInput &input) {
	if (input.child_stats.empty() || expr.StateExportMode() == AggregateStateExportMode::STATE_EXPORT) {
		return nullptr;
	}
	auto &child_stats = input.child_stats[0];
	auto &return_type = expr.GetReturnType();
	auto result = child_stats.GetType() == return_type
	                  ? child_stats.ToUnique()
	                  : CastStatistics::TryPropagate(child_stats, child_stats.GetType(), return_type);
	if (!result) {
		return nullptr;
	}
	result->ResetAdditiveStatistics();
	// the result is NULL when the aggregate sees no valid rows
	result->Set(StatsInfo::CAN_HAVE_NULL_VALUES);
	return result;
}

AggregateInputData::AggregateInputData(const BoundAggregateExpression &expr, ArenaAllocator &allocator_p,
                                       AggregateCombineType combine_type_p)
    : AggregateInputData(expr.Function(), expr.BindInfo().get(), allocator_p, combine_type_p) {
}

AggregateInputData::AggregateInputData(const AggregateObject &aggr, ArenaAllocator &allocator_p,
                                       AggregateCombineType combine_type_p)
    : AggregateInputData(aggr.function, aggr.bind_data_wrapper ? aggr.bind_data_wrapper->function_data.get() : nullptr,
                         allocator_p, combine_type_p) {
}

bool AggregateExecutor::CombineNeverRetainsSource(const AggregateInputData &aggr_input_data) {
	return aggr_input_data.function.GetCombineSourceRetention() == AggregateCombineSourceRetention::NEVER_RETAINS;
}

AggregateFinalizeInputData::AggregateFinalizeInputData(const BoundAggregateFunction &function_p,
                                                       optional_ptr<const FunctionData> bind_data_p,
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

namespace {

template <idx_t WIDTH, idx_t FLAG_COUNT>
void StateLayoutFinalizeLoop(Vector &states, AggregateFinalizeInputData &finalize_input_data, Vector &result,
                             idx_t count, idx_t offset) {
	auto &read = *finalize_input_data.function.GetStateValueRead();
	D_ASSERT(read.value_width == WIDTH && read.flag_count == FLAG_COUNT);
	D_ASSERT(WIDTH == GetTypeIdSize(result.GetType().InternalType()));
	if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		FlatVector::SetSize(result, count);
		auto state = *ConstantVector::GetData<data_ptr_t>(states);
		if (!read.Read<WIDTH, FLAG_COUNT>(state, ConstantVector::GetData(result))) {
			ConstantVector::SetNull(result, count_t(count));
		}
		return;
	}
	D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto sdata = FlatVector::GetData<data_ptr_t>(states);
	auto rdata = FlatVector::GetDataMutable(result);
	for (idx_t i = 0; i < count; i++) {
		const auto result_idx = i + offset;
		if (!read.Read<WIDTH, FLAG_COUNT>(sdata[i], rdata + result_idx * WIDTH)) {
			FlatVector::SetNull(result, result_idx, true);
		}
	}
}

template <idx_t WIDTH>
AggregateStateValueRead::finalize_t StateLayoutFinalizeLoopForFlags(idx_t flag_count) {
	switch (flag_count) {
	case 0:
		return StateLayoutFinalizeLoop<WIDTH, 0>;
	case 1:
		return StateLayoutFinalizeLoop<WIDTH, 1>;
	case 2:
		return StateLayoutFinalizeLoop<WIDTH, 2>;
	default:
		return nullptr;
	}
}

AggregateStateValueRead::finalize_t StateLayoutFinalizeLoopFor(idx_t width, idx_t flag_count) {
	switch (width) {
	case 1:
		return StateLayoutFinalizeLoopForFlags<1>(flag_count);
	case 2:
		return StateLayoutFinalizeLoopForFlags<2>(flag_count);
	case 4:
		return StateLayoutFinalizeLoopForFlags<4>(flag_count);
	case 8:
		return StateLayoutFinalizeLoopForFlags<8>(flag_count);
	case 16:
		return StateLayoutFinalizeLoopForFlags<16>(flag_count);
	default:
		return nullptr;
	}
}

} // namespace

AggregateStateValueRead AggregateStateValueRead::FromField(const AggregateStateField &root) {
	AggregateStateValueRead result;
	// Optional wrappers share their parent's base, struct members are relative to the struct's base
	idx_t base = 0;
	reference<const AggregateStateField> field = root;
	while (true) {
		auto &current = field.get();
		switch (current.kind) {
		case AggregateFieldKind::OPTIONAL_VALUE:
			if (result.flag_count == MAX_FLAGS || current.children.size() != 1) {
				throw InternalException("AggregateStateValueRead: unsupported optional nesting");
			}
			result.flag_offsets[result.flag_count++] = base + current.field_offset;
			field = current.children[0];
			continue;
		case AggregateFieldKind::STRUCT:
			if (current.children.size() != 1) {
				throw InternalException("AggregateStateValueRead: the state has more than one field");
			}
			base += current.field_offset;
			field = current.children[0];
			continue;
		case AggregateFieldKind::PRIMITIVE:
			result.value_offset = base + current.field_offset;
			result.value_width = current.field_size;
			result.finalize = StateLayoutFinalizeLoopFor(result.value_width, result.flag_count);
			if (!result.finalize) {
				throw InternalException("AggregateStateValueRead: unsupported value width %llu", result.value_width);
			}
			return result;
		default:
			throw InternalException("AggregateStateValueRead: the state holds no plain value");
		}
	}
}

bool AggregateFunctionProperties::operator==(const AggregateFunctionProperties &rhs) const {
	return FunctionProperties::operator==(rhs) && order_dependent == rhs.order_dependent &&
	       distinct_dependent == rhs.distinct_dependent && single_value_identity == rhs.single_value_identity &&
	       is_holistic == rhs.is_holistic && combine_source_retention == rhs.combine_source_retention &&
	       can_stream == rhs.can_stream;
}
bool AggregateFunctionProperties::operator!=(const AggregateFunctionProperties &rhs) const {
	return !(*this == rhs);
}

bool AggregateFunctionCallbacks::operator==(const AggregateFunctionCallbacks &rhs) const {
	return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
	       combine == rhs.combine && finalize == rhs.finalize &&
	       init_local_state_finalize == rhs.init_local_state_finalize && cluster_update == rhs.cluster_update &&
	       window == rhs.window && window_init == rhs.window_init && window_batch == rhs.window_batch &&
	       bind == rhs.bind && unbind == rhs.unbind && destructor == rhs.destructor && statistics == rhs.statistics &&
	       serialize == rhs.serialize && deserialize == rhs.deserialize && direct_rewrite == rhs.direct_rewrite &&
	       rewrite == rhs.rewrite && rewrite_policy == rhs.rewrite_policy &&
	       rewrite_optimizer_type == rhs.rewrite_optimizer_type && rewrite_cost == rhs.rewrite_cost &&
	       get_state_type == rhs.get_state_type && state_value_read == rhs.state_value_read &&
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

BoundAggregateFunction::BoundAggregateFunction(const AggregateFunction &function)
    // the function does not come from a function set - copy it into a definition of its own
    : BoundAggregateFunction(make_shared_ptr<AggregateFunction>(function)) {
}

BoundAggregateFunction::BoundAggregateFunction(shared_ptr<const AggregateFunction> function_p)
    : definition(std::move(function_p)) {
	auto &function = *definition;
	qualified_name = function.GetQualifiedName();
	extra_info = function.extra_info;
	return_type = function.GetReturnType();
	properties = function.GetProperties();
	callbacks = function.GetCallbacks();
	function_info = function.GetFunctionInfo();

	// Try to default bind the function, to fill in any missing information in the BoundScalarFunction (e.g. from the
	// "bind" callback)
	for (auto &param : function.GetSignature().GetParameters()) {
		if (!param.IsVariadic()) {
			arguments.push_back(param.GetType());
		}
	}
	positional_arguments = arguments.size();
	logical_arguments = arguments;
	logical_return_type = return_type;
}

bool BoundAggregateFunction::operator==(const BoundAggregateFunction &rhs) const {
	return callbacks == rhs.callbacks && properties == rhs.properties && arguments == rhs.arguments &&
	       positional_arguments == rhs.positional_arguments && named_arguments == rhs.named_arguments &&
	       return_type == rhs.return_type;
}
bool BoundAggregateFunction::operator!=(const BoundAggregateFunction &rhs) const {
	return !(*this == rhs);
}

void BoundAggregateFunction::ReplaceImplementation(const BoundAggregateFunction &function) {
	BaseAggregateFunction::operator=(function);
	BoundSimpleFunction::operator=(function);
	SetDefinition(definition);
}

void BoundAggregateFunction::ReplaceImplementation(const AggregateFunction &function) {
	SetName(function.GetName());
	// The replacement is a specialized implementation of the function we were bound from, and is usually built by
	// a factory rather than handed out by a catalog entry. Only take its qualification when it has one, so that
	// specializing an implementation does not drop the catalog and schema name of the definition.
	if (!function.GetCatalogName().empty() || !function.GetSchemaName().empty()) {
		qualified_name = function.GetQualifiedName();
	}
	this->return_type = function.GetReturnType();
	this->properties = function.GetProperties();
	this->callbacks = function.GetCallbacks();
	this->function_info = function.GetFunctionInfo();

	// Try to default bind the function, to fill in any missing information in the BoundScalarFunction (e.g. from the
	// "bind" callback)
	arguments.clear();
	for (auto &param : function.GetSignature().GetParameters()) {
		if (!param.IsVariadic()) {
			arguments.push_back(param.GetType());
		}
	}
}

BindAggregateFunctionInput::BindAggregateFunctionInput(ClientContext &context_p,
                                                       BoundAggregateFunction &bound_function_p,
                                                       vector<unique_ptr<Expression>> &arguments_p,
                                                       optional_ptr<Binder> binder_p,
                                                       optional_ptr<const DecimalWidthLimit> decimal_width_limit_p,
                                                       bool arguments_collated_p)
    : BindFunctionInput(context_p, bound_function_p, bound_function_p.GetDefinition()->GetSignature(), arguments_p,
                        binder_p, decimal_width_limit_p),
      bound_function(bound_function_p), arguments_collated(arguments_collated_p) {
}

} // namespace duckdb
