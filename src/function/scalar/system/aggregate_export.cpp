#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

// aggregate state export
struct ExportAggregateBindData : public FunctionData {
	AggregateFunction &aggr;
	idx_t state_size;

	explicit ExportAggregateBindData(AggregateFunction &aggr_p, idx_t state_size_p)
	    : aggr(aggr_p), state_size(state_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ExportAggregateBindData>(aggr, state_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const ExportAggregateBindData &)other_p;
		return aggr == other.aggr && state_size == other.state_size;
	}

	static ExportAggregateBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		return (ExportAggregateBindData &)*func_expr.bind_info;
	}
};

struct CombineState : public FunctionLocalState {
	idx_t state_size;

	unique_ptr<data_t[]> state_buffer0, state_buffer1;
	Vector state_vector0, state_vector1;

	explicit CombineState(idx_t state_size_p)
	    : state_size(state_size_p), state_buffer0(unique_ptr<data_t[]>(new data_t[state_size_p])),
	      state_buffer1(unique_ptr<data_t[]>(new data_t[state_size_p])),
	      state_vector0(Value::POINTER((uintptr_t)state_buffer0.get())),
	      state_vector1(Value::POINTER((uintptr_t)state_buffer1.get())) {
	}
};

static unique_ptr<FunctionLocalState> InitCombineState(const BoundFunctionExpression &expr, FunctionData *bind_data_p) {
	auto &bind_data = *(ExportAggregateBindData *)bind_data_p;
	return make_unique<CombineState>(bind_data.state_size);
}

struct FinalizeState : public FunctionLocalState {
	idx_t state_size;
	unique_ptr<data_t[]> state_buffer;
	Vector addresses;

	explicit FinalizeState(idx_t state_size_p)
	    : state_size(state_size_p),
	      state_buffer(unique_ptr<data_t[]>(new data_t[STANDARD_VECTOR_SIZE * AlignValue(state_size_p)])),
	      addresses(LogicalType::POINTER) {
	}
};

static unique_ptr<FunctionLocalState> InitFinalizeState(const BoundFunctionExpression &expr,
                                                        FunctionData *bind_data_p) {
	auto &bind_data = *(ExportAggregateBindData *)bind_data_p;
	return make_unique<FinalizeState>(bind_data.state_size);
}

static void AggregateStateFinalize(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = (FinalizeState &)*((ExecuteFunctionState &)state_p).local_state;

	D_ASSERT(bind_data.state_size == bind_data.aggr.state_size());
	D_ASSERT(input.data.size() == 1);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	auto aligned_state_size = AlignValue(bind_data.state_size);

	auto state_vec_ptr = FlatVector::GetData<data_ptr_t>(local_state.addresses);

	VectorData state_data;
	input.data[0].Orrify(input.size(), state_data);
	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		auto state_entry = &((string_t *)state_data.data)[state_idx];
		auto target_ptr = (const char *)local_state.state_buffer.get() + aligned_state_size * i;

		if (state_data.validity.RowIsValid(state_idx)) {
			D_ASSERT(state_entry->GetSize() == bind_data.state_size);
			memcpy((void *)target_ptr, state_entry->GetDataUnsafe(), bind_data.state_size);
		} else {
			// create a dummy state because finalize does not understand NULLs in its input
			// we put the NULL back in explicitly below
			bind_data.aggr.initialize((data_ptr_t)target_ptr);
		}
		state_vec_ptr[i] = (data_ptr_t)target_ptr;
	}

	bind_data.aggr.finalize(local_state.addresses, nullptr, result, input.size(), 0);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state_idx = state_data.sel->get_index(i);
		if (!state_data.validity.RowIsValid(state_idx)) {
			FlatVector::SetNull(result, i, true);
		}
	}
}

static void AggregateStateCombine(DataChunk &input, ExpressionState &state_p, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state_p);
	auto &local_state = (CombineState &)*((ExecuteFunctionState &)state_p).local_state;

	D_ASSERT(bind_data.state_size == bind_data.aggr.state_size());

	D_ASSERT(input.data.size() == 2);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	D_ASSERT(input.data[0].GetType() == result.GetType());

	if (input.data[0].GetType().InternalType() != input.data[1].GetType().InternalType()) {
		throw IOException("Aggregate state combine type mismatch, expect %s, got %s",
		                  input.data[0].GetType().ToString(), input.data[1].GetType().ToString());
	}

	VectorData state0_data, state1_data;
	input.data[0].Orrify(input.size(), state0_data);
	input.data[1].Orrify(input.size(), state1_data);

	auto result_ptr = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < input.size(); i++) {
		auto state0_idx = state0_data.sel->get_index(i);
		auto state1_idx = state1_data.sel->get_index(i);

		auto &state0 = ((string_t *)state0_data.data)[state0_idx];
		auto &state1 = ((string_t *)state1_data.data)[state1_idx];

		// if both are NULL, we return NULL. If either of them is not, the result is that one
		if (!state0_data.validity.RowIsValid(state0_idx) && !state1_data.validity.RowIsValid(state1_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		if (state0_data.validity.RowIsValid(state0_idx) && !state1_data.validity.RowIsValid(state1_idx)) {
			result_ptr[i] =
			    StringVector::AddStringOrBlob(result, (const char *)state0.GetDataUnsafe(), bind_data.state_size);
			continue;
		}
		if (!state0_data.validity.RowIsValid(state0_idx) && state1_data.validity.RowIsValid(state1_idx)) {
			result_ptr[i] =
			    StringVector::AddStringOrBlob(result, (const char *)state1.GetDataUnsafe(), bind_data.state_size);
			continue;
		}

		// we actually have to combine
		if (state0.GetSize() != bind_data.state_size || state1.GetSize() != bind_data.state_size) {
			throw IOException("Aggregate state size mismatch, expect %llu, got %llu and %llu", bind_data.state_size,
			                  state0.GetSize(), state1.GetSize());
		}

		memcpy(local_state.state_buffer0.get(), state0.GetDataUnsafe(), bind_data.state_size);
		memcpy(local_state.state_buffer1.get(), state1.GetDataUnsafe(), bind_data.state_size);

		bind_data.aggr.combine(local_state.state_vector0, local_state.state_vector1, nullptr, 1);

		result_ptr[i] =
		    StringVector::AddStringOrBlob(result, (const char *)local_state.state_buffer1.get(), bind_data.state_size);
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

	// following error states are only reachable when someone messes up creating the state_type which is impossible from
	// SQL

	auto state_type = AggregateStateType::GetStateType(arg_return_type);

	// now we can look up the function in the catalog again and bind it
	auto func = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA,
	                                                  state_type.function_name);
	if (func->type != CatalogType::AGGREGATE_FUNCTION_ENTRY) {
		throw InternalException("Could not find aggregate %s", state_type.function_name);
	}
	auto aggr = (AggregateFunctionCatalogEntry *)func;

	string error;
	idx_t best_function = Function::BindFunction(aggr->name, aggr->functions, state_type.bound_argument_types, error);
	if (best_function == DConstants::INVALID_INDEX) {
		throw InternalException("Could not re-bind exported aggregate %s: %s", state_type.function_name, error);
	}
	auto &bound_aggr = aggr->functions[best_function];
	if (bound_aggr.return_type != state_type.return_type || bound_aggr.arguments != state_type.bound_argument_types) {
		throw InternalException("Type mismatch for exported aggregate %s", state_type.function_name);
	}

	if (bound_function.name == "finalize") {
		bound_function.return_type = bound_aggr.return_type;
	} else {
		D_ASSERT(bound_function.name == "combine");
		bound_function.return_type = arg_return_type;
	}

	return make_unique<ExportAggregateBindData>(bound_aggr, bound_aggr.state_size());
}

static void ExportAggregateFinalize(Vector &state, FunctionData *bind_data_p, Vector &result, idx_t count,
                                    idx_t offset) {
	D_ASSERT(offset == 0);
	auto bind_data = (ExportAggregateFunctionBindData *)bind_data_p;
	auto state_size = bind_data->aggregate->function.state_size();
	auto blob_ptr = FlatVector::GetData<string_t>(result);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(state);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto data_ptr = addresses_ptr[row_idx];
		blob_ptr[row_idx] = StringVector::AddStringOrBlob(result, (const char *)data_ptr, state_size);
	}
}

ExportAggregateFunctionBindData::ExportAggregateFunctionBindData(unique_ptr<Expression> aggregate_p) {
	D_ASSERT(aggregate_p->type == ExpressionType::BOUND_AGGREGATE);
	aggregate = unique_ptr<BoundAggregateExpression>((BoundAggregateExpression *)aggregate_p.release());
}

unique_ptr<FunctionData> ExportAggregateFunctionBindData::Copy() const {
	return make_unique<ExportAggregateFunctionBindData>(aggregate->Copy());
}

bool ExportAggregateFunctionBindData::Equals(const FunctionData &other_p) const {
	auto &other = (const ExportAggregateFunctionBindData &)other_p;
	return aggregate->Equals(other.aggregate.get());
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
	D_ASSERT(!bound_function.window);

	D_ASSERT(child_aggregate->function.return_type.id() != LogicalTypeId::INVALID);
#ifdef DEBUG
	for (auto &arg_type : child_aggregate->function.arguments) {
		D_ASSERT(arg_type.id() != LogicalTypeId::INVALID);
	}
#endif
	auto export_bind_data = make_unique<ExportAggregateFunctionBindData>(child_aggregate->Copy());
	aggregate_state_t state_type(child_aggregate->function.name, child_aggregate->function.return_type,
	                             child_aggregate->function.arguments);
	auto return_type = LogicalType::AGGREGATE_STATE(move(state_type));

	auto export_function =
	    AggregateFunction("aggregate_state_export_" + bound_function.name, bound_function.arguments, return_type,
	                      bound_function.state_size, bound_function.initialize, bound_function.update,
	                      bound_function.combine, ExportAggregateFinalize, bound_function.simple_update,
	                      /* can't bind this again */ nullptr, /* no dynamic state yet */ nullptr,
	                      /* can't propagate statistics */ nullptr, nullptr);

	return make_unique<BoundAggregateExpression>(export_function, move(child_aggregate->children),
	                                             move(child_aggregate->filter), move(export_bind_data),
	                                             child_aggregate->distinct);
}

ScalarFunction ExportAggregateFunction::GetFinalize() {
	return ScalarFunction("finalize", {LogicalTypeId::AGGREGATE_STATE}, LogicalTypeId::INVALID, AggregateStateFinalize,
	                      false, BindAggregateState, nullptr, nullptr, InitFinalizeState);
}

ScalarFunction ExportAggregateFunction::GetCombine() {
	return ScalarFunction("combine", {LogicalTypeId::AGGREGATE_STATE, LogicalTypeId::ANY},
	                      LogicalTypeId::AGGREGATE_STATE, AggregateStateCombine, false, BindAggregateState, nullptr,
	                      nullptr, InitCombineState);
}

} // namespace duckdb
