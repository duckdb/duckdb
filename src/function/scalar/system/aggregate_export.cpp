#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

// aggregate state export
struct ExportAggregateBindData : public FunctionData {
	AggregateFunction aggr;
	idx_t state_size;

	explicit ExportAggregateBindData(AggregateFunction aggr_p, idx_t state_size_p)
	    : aggr(std::move(aggr_p)), state_size(state_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExportAggregateBindData>(aggr, state_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExportAggregateBindData>();
		return aggr == other.aggr && state_size == other.state_size;
	}

	static ExportAggregateBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		return func_expr.bind_info->Cast<ExportAggregateBindData>();
	}
};

struct CombineState : public FunctionLocalState {
	idx_t state_size;

	unsafe_unique_array<data_t> state_buffer0, state_buffer1;
	Vector state_vector0, state_vector1;

	ArenaAllocator allocator;

	explicit CombineState(idx_t state_size_p)
	    : state_size(state_size_p), state_buffer0(make_unsafe_uniq_array<data_t>(state_size_p)),
	      state_buffer1(make_unsafe_uniq_array<data_t>(state_size_p)),
	      state_vector0(Value::POINTER(CastPointerToValue(state_buffer0.get()))),
	      state_vector1(Value::POINTER(CastPointerToValue(state_buffer1.get()))),
	      allocator(Allocator::DefaultAllocator()) {
	}
};

static unique_ptr<FunctionLocalState> InitCombineState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                       FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<CombineState>(bind_data.state_size);
}

struct FinalizeState : public FunctionLocalState {
	idx_t state_size;
	unsafe_unique_array<data_t> state_buffer;
	Vector addresses;

	ArenaAllocator allocator;

	explicit FinalizeState(idx_t state_size_p)
	    : state_size(state_size_p),
	      state_buffer(make_unsafe_uniq_array<data_t>(STANDARD_VECTOR_SIZE * AlignValue(state_size_p))),
	      addresses(LogicalType::POINTER), allocator(Allocator::DefaultAllocator()) {
	}
};

static unique_ptr<FunctionLocalState> InitFinalizeState(ExpressionState &state, const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ExportAggregateBindData>();
	return make_uniq<FinalizeState>(bind_data.state_size);
}

static void AggregateStateFinalize(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<FinalizeState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.state_size(bind_data.aggr));
	D_ASSERT(input.data.size() == 1);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	auto aligned_state_size = AlignValue(bind_data.state_size);

	auto state_vec_ptr = FlatVector::GetData<data_ptr_t>(local_state.addresses);

	UnifiedVectorFormat state_data;
	input.data[0].ToUnifiedFormat(input.size(), state_data);
	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		auto state_entry = UnifiedVectorFormat::GetData<string_t>(state_data) + state_idx;
		auto target_ptr = char_ptr_cast(local_state.state_buffer.get()) + aligned_state_size * i;

		if (state_data.validity.RowIsValid(state_idx)) {
			D_ASSERT(state_entry->GetSize() == bind_data.state_size);
			memcpy((void *)target_ptr, state_entry->GetData(), bind_data.state_size);
		} else {
			// create a dummy state because finalize does not understand NULLs in its input
			// we put the NULL back in explicitly below
			bind_data.aggr.initialize(bind_data.aggr, data_ptr_cast(target_ptr));
		}
		state_vec_ptr[i] = data_ptr_cast(target_ptr);
	}

	AggregateInputData aggr_input_data(nullptr, local_state.allocator);
	bind_data.aggr.finalize(local_state.addresses, aggr_input_data, result, input.size(), 0);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		if (!state_data.validity.RowIsValid(state_idx)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

static void AggregateStateCombine(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = ExecuteFunctionState::GetFunctionState(state_p)->Cast<CombineState>();
	local_state.allocator.Reset();

	D_ASSERT(bind_data.state_size == bind_data.aggr.state_size(bind_data.aggr));

	D_ASSERT(input.data.size() == 2);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	D_ASSERT(input.data[0].GetType() == result.GetType());

	if (input.data[0].GetType().InternalType() != input.data[1].GetType().InternalType()) {
		throw IOException("Aggregate state combine type mismatch, expect %s, got %s",
		                  input.data[0].GetType().ToString(), input.data[1].GetType().ToString());
	}

	UnifiedVectorFormat state0_data, state1_data;
	input.data[0].ToUnifiedFormat(input.size(), state0_data);
	input.data[1].ToUnifiedFormat(input.size(), state1_data);

	auto result_ptr = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state0_idx = state0_data.sel->get_index(i);
		auto state1_idx = state1_data.sel->get_index(i);

		auto &state0 = UnifiedVectorFormat::GetData<string_t>(state0_data)[state0_idx];
		auto &state1 = UnifiedVectorFormat::GetData<string_t>(state1_data)[state1_idx];

		// if both are NULL, we return NULL. If either of them is not, the result is that one
		if (!state0_data.validity.RowIsValid(state0_idx) && !state1_data.validity.RowIsValid(state1_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		if (state0_data.validity.RowIsValid(state0_idx) && !state1_data.validity.RowIsValid(state1_idx)) {
			result_ptr[i] =
			    StringVector::AddStringOrBlob(result, const_char_ptr_cast(state0.GetData()), bind_data.state_size);
			continue;
		}
		if (!state0_data.validity.RowIsValid(state0_idx) && state1_data.validity.RowIsValid(state1_idx)) {
			result_ptr[i] =
			    StringVector::AddStringOrBlob(result, const_char_ptr_cast(state1.GetData()), bind_data.state_size);
			continue;
		}

		// we actually have to combine
		if (state0.GetSize() != bind_data.state_size || state1.GetSize() != bind_data.state_size) {
			throw IOException("Aggregate state size mismatch, expect %llu, got %llu and %llu", bind_data.state_size,
			                  state0.GetSize(), state1.GetSize());
		}

		memcpy(local_state.state_buffer0.get(), state0.GetData(), bind_data.state_size);
		memcpy(local_state.state_buffer1.get(), state1.GetData(), bind_data.state_size);

		AggregateInputData aggr_input_data(nullptr, local_state.allocator, AggregateCombineType::ALLOW_DESTRUCTIVE);
		bind_data.aggr.combine(local_state.state_vector0, local_state.state_vector1, aggr_input_data, 1);

		result_ptr[i] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(local_state.state_buffer1.get()),
		                                              bind_data.state_size);
	}
}

static unique_ptr<FunctionData> BindAggregateState(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {

	// grab the aggregate type and bind the aggregate again

	// the aggregate name and types are in the logical type of the aggregate state, make sure its sane
	auto &arg_return_type = arguments[0]->return_type;
	for (auto &arg_type : bound_function.arguments) {
		arg_type = arg_return_type;
	}

	if (arg_return_type.id() != LogicalTypeId::AGGREGATE_STATE) {
		throw BinderException("Can only FINALIZE aggregate state, not %s", arg_return_type.ToString());
	}
	// combine
	if (arguments.size() == 2 && arguments[0]->return_type != arguments[1]->return_type &&
	    arguments[1]->return_type.id() != LogicalTypeId::BLOB) {
		throw BinderException("Cannot COMBINE aggregate states from different functions, %s <> %s",
		                      arguments[0]->return_type.ToString(), arguments[1]->return_type.ToString());
	}

	// following error states are only reachable when someone messes up creating the state_type
	// which is impossible from SQL

	auto state_type = AggregateStateType::GetStateType(arg_return_type);

	// now we can look up the function in the catalog again and bind it
	auto &func = Catalog::GetSystemCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY,
	                                                         DEFAULT_SCHEMA, state_type.function_name);
	if (func.type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw InternalException("Could not find aggregate %s", state_type.function_name);
	}
	auto &aggr = func.Cast<AggregateFunctionCatalogEntry>();

	ErrorData error;

	FunctionBinder function_binder(context);
	auto best_function =
	    function_binder.BindFunction(aggr.name, aggr.functions, state_type.bound_argument_types, error);
	if (!best_function.IsValid()) {
		throw InternalException("Could not re-bind exported aggregate %s: %s", state_type.function_name,
		                        error.Message());
	}
	auto bound_aggr = aggr.functions.GetFunctionByOffset(best_function.GetIndex());
	if (bound_aggr.bind) {
		// FIXME: this is really hacky
		// but the aggregate state export needs a rework around how it handles more complex aggregates anyway
		vector<unique_ptr<Expression>> args;
		args.reserve(state_type.bound_argument_types.size());
		for (auto &arg_type : state_type.bound_argument_types) {
			args.push_back(make_uniq<BoundConstantExpression>(Value(arg_type)));
		}
		auto bind_info = bound_aggr.bind(context, bound_aggr, args);
		if (bind_info) {
			throw BinderException("Aggregate function with bind info not supported yet in aggregate state export");
		}
	}

	if (bound_aggr.return_type != state_type.return_type || bound_aggr.arguments != state_type.bound_argument_types) {
		throw InternalException("Type mismatch for exported aggregate %s", state_type.function_name);
	}

	if (bound_function.name == "finalize") {
		bound_function.return_type = bound_aggr.return_type;
	} else {
		D_ASSERT(bound_function.name == "combine");
		bound_function.return_type = arg_return_type;
	}

	return make_uniq<ExportAggregateBindData>(bound_aggr, bound_aggr.state_size(bound_aggr));
}

static void ExportAggregateFinalize(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                                    idx_t offset) {
	D_ASSERT(offset == 0);
	auto &bind_data = aggr_input_data.bind_data->Cast<ExportAggregateFunctionBindData>();
	auto state_size = bind_data.aggregate->function.state_size(bind_data.aggregate->function);
	auto blob_ptr = FlatVector::GetData<string_t>(result);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(state);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto data_ptr = addresses_ptr[row_idx];
		blob_ptr[row_idx] = StringVector::AddStringOrBlob(result, const_char_ptr_cast(data_ptr), state_size);
	}
}

ExportAggregateFunctionBindData::ExportAggregateFunctionBindData(unique_ptr<Expression> aggregate_p) {
	D_ASSERT(aggregate_p->type == ExpressionType::BOUND_AGGREGATE);
	aggregate = unique_ptr_cast<Expression, BoundAggregateExpression>(std::move(aggregate_p));
}

unique_ptr<FunctionData> ExportAggregateFunctionBindData::Copy() const {
	return make_uniq<ExportAggregateFunctionBindData>(aggregate->Copy());
}

bool ExportAggregateFunctionBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ExportAggregateFunctionBindData>();
	return aggregate->Equals(*other.aggregate);
}

static void ExportStateAggregateSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                          const AggregateFunction &function) {
	throw NotImplementedException("FIXME: export state serialize");
}

static unique_ptr<FunctionData> ExportStateAggregateDeserialize(Deserializer &deserializer,
                                                                AggregateFunction &function) {
	throw NotImplementedException("FIXME: export state deserialize");
}

static void ExportStateScalarSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                       const ScalarFunction &function) {
	throw NotImplementedException("FIXME: export state serialize");
}

static unique_ptr<FunctionData> ExportStateScalarDeserialize(Deserializer &deserializer, ScalarFunction &function) {
	throw NotImplementedException("FIXME: export state deserialize");
}

unique_ptr<BoundAggregateExpression>
ExportAggregateFunction::Bind(unique_ptr<BoundAggregateExpression> child_aggregate) {
	auto &bound_function = child_aggregate->function;
	if (!bound_function.combine) {
		throw BinderException("Cannot use EXPORT_STATE for non-combinable function %s", bound_function.name);
	}
	if (bound_function.bind) {
		throw BinderException("Cannot use EXPORT_STATE on aggregate functions with custom binders");
	}
	if (bound_function.destructor) {
		throw BinderException("Cannot use EXPORT_STATE on aggregate functions with custom destructors");
	}
	// this should be required
	D_ASSERT(bound_function.state_size);
	D_ASSERT(bound_function.finalize);

	D_ASSERT(child_aggregate->function.return_type.id() != LogicalTypeId::INVALID);
#ifdef DEBUG
	for (auto &arg_type : child_aggregate->function.arguments) {
		D_ASSERT(arg_type.id() != LogicalTypeId::INVALID);
	}
#endif
	auto export_bind_data = make_uniq<ExportAggregateFunctionBindData>(child_aggregate->Copy());
	aggregate_state_t state_type(child_aggregate->function.name, child_aggregate->function.return_type,
	                             child_aggregate->function.arguments);
	auto return_type = LogicalType::AGGREGATE_STATE(std::move(state_type));

	auto export_function =
	    AggregateFunction("aggregate_state_export_" + bound_function.name, bound_function.arguments, return_type,
	                      bound_function.state_size, bound_function.initialize, bound_function.update,
	                      bound_function.combine, ExportAggregateFinalize, bound_function.simple_update,
	                      /* can't bind this again */ nullptr, /* no dynamic state yet */ nullptr,
	                      /* can't propagate statistics */ nullptr, nullptr);
	export_function.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	export_function.serialize = ExportStateAggregateSerialize;
	export_function.deserialize = ExportStateAggregateDeserialize;

	return make_uniq<BoundAggregateExpression>(export_function, std::move(child_aggregate->children),
	                                           std::move(child_aggregate->filter), std::move(export_bind_data),
	                                           child_aggregate->aggr_type);
}

ScalarFunction ExportAggregateFunction::GetFinalize() {
	auto result = ScalarFunction("finalize", {LogicalTypeId::AGGREGATE_STATE}, LogicalTypeId::INVALID,
	                             AggregateStateFinalize, BindAggregateState, nullptr, nullptr, InitFinalizeState);
	result.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	result.serialize = ExportStateScalarSerialize;
	result.deserialize = ExportStateScalarDeserialize;
	return result;
}

ScalarFunction ExportAggregateFunction::GetCombine() {
	auto result =
	    ScalarFunction("combine", {LogicalTypeId::AGGREGATE_STATE, LogicalTypeId::ANY}, LogicalTypeId::AGGREGATE_STATE,
	                   AggregateStateCombine, BindAggregateState, nullptr, nullptr, InitCombineState);
	result.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	result.serialize = ExportStateScalarSerialize;
	result.deserialize = ExportStateScalarDeserialize;
	return result;
}

void ExportAggregateFunction::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ExportAggregateFunction::GetCombine());
	set.AddFunction(ExportAggregateFunction::GetFinalize());
}

} // namespace duckdb
