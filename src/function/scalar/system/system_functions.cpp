#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/function/scalar/generic_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

// current_query
struct SystemBindData : public FunctionData {
	ClientContext &context;

	explicit SystemBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<SystemBindData>(context);
	}

	static SystemBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		return (SystemBindData &)*func_expr.bind_info;
	}
};

unique_ptr<FunctionData> BindSystemFunction(ClientContext &context, ScalarFunction &bound_function,
                                            vector<unique_ptr<Expression>> &arguments) {
	return make_unique<SystemBindData>(context);
}

static void CurrentQueryFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &info = SystemBindData::GetFrom(state);
	Value val(info.context.GetCurrentQuery());
	result.Reference(val);
}

// current_schema
static void CurrentSchemaFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	Value val(SystemBindData::GetFrom(state).context.catalog_search_path->GetDefault());
	result.Reference(val);
}

// current_schemas
static void CurrentSchemasFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	vector<Value> schema_list;
	vector<string> search_path = SystemBindData::GetFrom(state).context.catalog_search_path->Get();
	std::transform(search_path.begin(), search_path.end(), std::back_inserter(schema_list),
	               [](const string &s) -> Value { return Value(s); });
	auto val = Value::LIST(schema_list);
	result.Reference(val);
}

// txid_current
static void TransactionIdCurrent(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &transaction = Transaction::GetTransaction(SystemBindData::GetFrom(state).context);
	auto val = Value::BIGINT(transaction.start_time);
	result.Reference(val);
}

// version
static void VersionFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto val = Value(DuckDB::LibraryVersion());
	result.Reference(val);
}

// aggregate state export

struct ExportAggregateFinalizeBindData : public FunctionData {
	AggregateFunction &aggr;
	unique_ptr<FunctionData> bind_data;

	explicit ExportAggregateFinalizeBindData(AggregateFunction &aggr_p, unique_ptr<FunctionData> bind_data_p)
	    : aggr(aggr_p), bind_data(move(bind_data_p)) {
	}

	unique_ptr<FunctionData> Copy() override {
		if (bind_data) {
			return make_unique<ExportAggregateFinalizeBindData>(aggr, bind_data->Copy());
		}
		return make_unique<ExportAggregateFinalizeBindData>(aggr, nullptr);
	}

	static ExportAggregateFinalizeBindData &GetFrom(ExpressionState &state) {
		auto &func_expr = (BoundFunctionExpression &)state.expr;
		return (ExportAggregateFinalizeBindData &)*func_expr.bind_info;
	}
};

static void ExportAggregateFinalize(Vector &state, FunctionData *bind_data_p, Vector &result, idx_t count,
                                    idx_t offset) {
	auto bind_data = (ExportAggregateFunctionBindData *)bind_data_p;
	auto state_size = bind_data->function.state_size();
	auto blob_ptr = FlatVector::GetData<string_t>(result);
	auto addresses_ptr = FlatVector::GetData<data_ptr_t>(state);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto data_ptr = addresses_ptr[row_idx];
		blob_ptr[row_idx] = StringVector::AddStringOrBlob(result, (const char *)data_ptr, state_size);
	}
}

// this cannot be bound by name
AggregateFunction ExportAggregateFunction::GetFunction(LogicalType &return_type, AggregateFunction &bound_function) {
	return AggregateFunction("aggregate_state_export_" + bound_function.name, bound_function.arguments, return_type,
	                         bound_function.state_size, bound_function.initialize, bound_function.update,
	                         bound_function.combine, ExportAggregateFinalize, bound_function.simple_update,
	                         /* can't bind this */ nullptr, bound_function.destructor, bound_function.statistics,
	                         bound_function.window);
}

static void AggregateStateFinalize(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &bind_data = ExportAggregateFinalizeBindData::GetFrom(state);
	auto state_size = bind_data.aggr.state_size();
	D_ASSERT(input.data.size() == 1);
	D_ASSERT(input.data[0].GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	auto state_buf = malloc(state_size);
	Vector state_vec(Value::POINTER((uintptr_t)state_buf));
	Vector scalar_result(result.GetType());
	auto state_ptr = FlatVector::GetData<string_t>(input.data[0]);

	for (idx_t i = 0; i < input.size(); i++) {
		memcpy(state_buf, state_ptr[i].GetDataUnsafe(), state_size);
		bind_data.aggr.finalize(state_vec, bind_data.bind_data.get(), scalar_result, 1, 0);
		result.SetValue(i, scalar_result.GetValue(0)); // FIXME
	}
}

static unique_ptr<FunctionData> BindAggregateStateFinalize(ClientContext &context, ScalarFunction &bound_function,
                                                           vector<unique_ptr<Expression>> &arguments) {
	// grab the aggregate type and bind the aggregate again
	D_ASSERT(arguments.size() == 1);
	auto &arg_type = arguments[0]->return_type;
	D_ASSERT(arg_type.id() == LogicalTypeId::AGGREGATE_STATE);
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
	bound_function.return_type = bound_aggr.return_type;

	// construct fake expressions for the bind
	vector<unique_ptr<Expression>> aggr_args;
	for (auto &expr : state_type.bound_argument_types) {
		aggr_args.push_back(make_unique<BoundConstantExpression>(Value().CastAs(expr)));
	}

	unique_ptr<FunctionData> aggr_bind;
	if (bound_aggr.bind) {
		aggr_bind = bound_aggr.bind(context, bound_aggr, aggr_args);
	}
	return make_unique<ExportAggregateFinalizeBindData>(bound_aggr, move(aggr_bind));
}

void SystemFun::RegisterFunction(BuiltinFunctions &set) {
	auto varchar_list_type = LogicalType::LIST(LogicalType::VARCHAR);

	set.AddFunction(
	    ScalarFunction("current_query", {}, LogicalType::VARCHAR, CurrentQueryFunction, false, BindSystemFunction));
	set.AddFunction(
	    ScalarFunction("current_schema", {}, LogicalType::VARCHAR, CurrentSchemaFunction, false, BindSystemFunction));
	set.AddFunction(ScalarFunction("current_schemas", {LogicalType::BOOLEAN}, varchar_list_type, CurrentSchemasFunction,
	                               false, BindSystemFunction));
	set.AddFunction(
	    ScalarFunction("txid_current", {}, LogicalType::BIGINT, TransactionIdCurrent, false, BindSystemFunction));
	set.AddFunction(ScalarFunction("version", {}, LogicalType::VARCHAR, VersionFunction));
	set.AddFunction(ScalarFunction("finalize", {LogicalType::ANY}, LogicalType::INVALID, AggregateStateFinalize, false,
	                               BindAggregateStateFinalize));
}

} // namespace duckdb
