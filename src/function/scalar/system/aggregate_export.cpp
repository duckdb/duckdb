#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

// aggregate state export

struct ExportAggregateBindData : public FunctionData {
	AggregateFunction &aggr;
	unique_ptr<FunctionData> bind_data;

	explicit ExportAggregateBindData(AggregateFunction &aggr_p, unique_ptr<FunctionData> bind_data_p)
	    : aggr(aggr_p), bind_data(move(bind_data_p)) {
	}

	unique_ptr<FunctionData> Copy() override {
		if (bind_data) {
			return make_unique<ExportAggregateBindData>(aggr, bind_data->Copy());
		}
		return make_unique<ExportAggregateBindData>(aggr, nullptr);
	}

	static ExportAggregateBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		return (ExportAggregateBindData &)*func_expr.bind_info;
	}
};

static void AggregateStateFinalize(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state);
	auto state_size = bind_data.aggr.state_size();
	D_ASSERT(input.data.size() == 1);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	auto state_buf = malloc(state_size);
	Vector state_vec(Value::POINTER((uintptr_t)state_buf));
	Vector scalar_result(result.GetType());
	auto state_ptr = FlatVector::GetData<string_t>(input.data[0]);

	for (idx_t i = 0; i < input.size(); i++) {
		D_ASSERT(state_ptr[i].GetSize() == state_size);
		memcpy(state_buf, state_ptr[i].GetDataUnsafe(), state_size);
		bind_data.aggr.finalize(state_vec, bind_data.bind_data.get(), scalar_result, 1, 0);
		result.SetValue(i, scalar_result.GetValue(0)); // FIXME
	}
}

static void AggregateStateCombine(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &bind_data = ExportAggregateBindData::GetFrom(state);
	auto state_size = bind_data.aggr.state_size();

	D_ASSERT(input.data.size() == 2);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);
	D_ASSERT(input.data[0].GetType() == input.data[1].GetType());
	D_ASSERT(input.data[0].GetType() == result.GetType());

	// TODO put this into the state
	auto state_buf0 = malloc(state_size);
	auto state_buf1 = malloc(state_size);

	Vector state_vec0(Value::POINTER((uintptr_t)state_buf0));
	Vector state_vec1(Value::POINTER((uintptr_t)state_buf1));

	auto state_ptr0 = FlatVector::GetData<string_t>(input.data[0]);
	auto state_ptr1 = FlatVector::GetData<string_t>(input.data[1]);

	auto result_ptr = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < input.size(); i++) {
		D_ASSERT(state_ptr0[i].GetSize() == state_size);
		D_ASSERT(state_ptr1[i].GetSize() == state_size);

		memcpy(state_buf0, state_ptr0[i].GetDataUnsafe(), state_size);
		memcpy(state_buf1, state_ptr1[i].GetDataUnsafe(), state_size);

		bind_data.aggr.combine(state_vec0, state_vec1, 1);
		result_ptr[i] = StringVector::AddStringOrBlob(result, (const char *)state_buf1, state_size);
	}
}

static unique_ptr<FunctionData> BindAggregateState(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	// grab the aggregate type and bind the aggregate again
	auto &arg_type = arguments[0]->return_type;
	if (arg_type.id() != LogicalTypeId::AGGREGATE_STATE) {
		throw BinderException("Can only finalize aggregate state, not %s", arg_type.ToString());
	}
	// combine
	if (arguments.size() == 2 && arguments[0]->return_type != arguments[1]->return_type) {
		throw BinderException("Cannot COMBINE aggregate states from different functions, %s <> %s",
		                      arguments[0]->return_type.ToString(), arguments[1]->return_type.ToString());
	}

	auto state_type = AggregateStateType::GetStateType(arg_type);

	auto func = Catalog::GetCatalog(context).GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA,
	                                                  state_type.function_name);
	D_ASSERT(func->type == CatalogType::AGGREGATE_FUNCTION_ENTRY);
	auto aggr = (AggregateFunctionCatalogEntry *)func;
	string error;
	idx_t best_function = Function::BindFunction(aggr->name, aggr->functions, state_type.bound_argument_types, error);
	D_ASSERT(best_function != DConstants::INVALID_INDEX);
	// found a matching function!
	auto &bound_aggr = aggr->functions[best_function];

	// construct fake expressions for the bind
	vector<unique_ptr<Expression>> aggr_args;
	for (auto &expr : state_type.bound_argument_types) {
		aggr_args.push_back(make_unique<BoundConstantExpression>(Value().CastAs(expr)));
	}

	unique_ptr<FunctionData> aggr_bind;
	if (bound_aggr.bind) {
		aggr_bind = bound_aggr.bind(context, bound_aggr, aggr_args);
	}

	// TODO does this not change the return type in the catalog?
	if (bound_function.name == "finalize") {
		bound_function.return_type = bound_aggr.return_type;
	} else if (bound_function.name == "combine") {
		bound_function.return_type = arg_type;
	}

	return make_unique<ExportAggregateBindData>(bound_aggr, move(aggr_bind));
}

static void ExportAggregateFinalize(Vector &state, FunctionData *bind_data_p, Vector &result, idx_t count,
                                    idx_t offset) {
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

unique_ptr<FunctionData> ExportAggregateFunctionBindData::Copy() {
	return make_unique<ExportAggregateFunctionBindData>(aggregate->Copy());
}

unique_ptr<BoundAggregateExpression>
ExportAggregateFunction::Bind(unique_ptr<BoundAggregateExpression> child_aggregate) {
	auto &bound_function = child_aggregate->function;
	if (!bound_function.combine) {
		throw BinderException("Cannot export state for non-combinable function %s", bound_function.name);
	}
	if (bound_function.destructor) {
		throw BinderException("Cannot export state on functions with destructors");
	}
	// this should be required
	D_ASSERT(bound_function.state_size);
	D_ASSERT(bound_function.finalize);

	auto export_bind_data = make_unique<ExportAggregateFunctionBindData>(child_aggregate->Copy());
	aggregate_state_t state_type(child_aggregate->function.name, child_aggregate->function.return_type,
	                             child_aggregate->function.arguments);
	auto return_type = LogicalType::AGGREGATE_STATE(move(state_type));

	auto export_function =
	    AggregateFunction("aggregate_state_export_" + bound_function.name, bound_function.arguments, return_type,
	                      bound_function.state_size, bound_function.initialize, bound_function.update,
	                      bound_function.combine, ExportAggregateFinalize, bound_function.simple_update,
	                      /* can't bind this again */ nullptr, /* no dynamic state yet */ nullptr,
	                      /* can't propagate statistics */ nullptr, bound_function.window);

	return make_unique<BoundAggregateExpression>(export_function, move(child_aggregate->children),
	                                             move(child_aggregate->filter), move(export_bind_data),
	                                             child_aggregate->distinct);
}

ScalarFunction ExportAggregateFunction::GetFinalize() {
	return ScalarFunction("finalize", {LogicalType::ANY}, LogicalType::INVALID, AggregateStateFinalize, false,
	                      BindAggregateState);
}

ScalarFunction ExportAggregateFunction::GetCombine() {
	return ScalarFunction("combine", {LogicalType::ANY, LogicalType::ANY}, LogicalType::INVALID, AggregateStateCombine,
	                      false, BindAggregateState);
}

} // namespace duckdb
