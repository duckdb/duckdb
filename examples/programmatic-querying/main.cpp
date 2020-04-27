#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"

/**
This file contains an example on how a query tree can be programmatically constructed. This is essentially hand-rolling
the binding+planning phase for one specific query.

Note that this API is currently very unstable, and is subject to change at any moment. In general, this API should not
be used currently outside of internal use cases.
**/

using namespace duckdb;

struct MyScanFunctionData : public TableFunctionData {
	MyScanFunctionData() : nrow(10000) {
	}

	size_t nrow;
};

static unique_ptr<FunctionData> my_scan_bind(ClientContext &context, vector<Value> inputs,
                                             vector<SQLType> &return_types, vector<string> &names) {
	names.push_back("some_int");
	return_types.push_back(SQLType::INTEGER);

	names.push_back("some_string");
	return_types.push_back(SQLType::VARCHAR);

	return make_unique<MyScanFunctionData>();
}

void my_scan_function(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((MyScanFunctionData *)dataptr);
	assert(input.size() == 0);

	if (data.nrow < 1) {
		return;
	}

	// generate data for two output columns
	size_t this_rows = std::min(data.nrow, (size_t)STANDARD_VECTOR_SIZE);
	data.nrow -= this_rows;

	auto int_data = FlatVector::GetData<int32_t>(output.data[0]);
	for (size_t row = 0; row < this_rows; row++) {
		int_data[row] = row % 10;
	}
	output.SetCardinality(this_rows);
	for (size_t row = 0; row < this_rows; row++) {
		output.SetValue(1, row, Value("hello_" + std::to_string(row)));
	}
}

class MyScanFunction : public TableFunction {
public:
	MyScanFunction() : TableFunction("my_scan", {}, my_scan_bind, my_scan_function, nullptr){};
};

unique_ptr<BoundFunctionExpression> resolve_function(Connection &con, string name, vector<SQLType> function_args,
                                                     bool is_operator = true) {
	auto catalog_entry =
	    con.context->catalog.GetEntry(*con.context, CatalogType::SCALAR_FUNCTION, DEFAULT_SCHEMA, name, false);
	assert(catalog_entry->type == CatalogType::SCALAR_FUNCTION);
	auto scalar_fun = (ScalarFunctionCatalogEntry *)catalog_entry;

	idx_t best_function = Function::BindFunction(scalar_fun->name, scalar_fun->functions, function_args);
	auto fun = scalar_fun->functions[best_function];

	return make_unique<BoundFunctionExpression>(GetInternalType(fun.return_type), fun, is_operator);
}

unique_ptr<BoundAggregateExpression> resolve_aggregate(Connection &con, string name, vector<SQLType> function_args) {
	auto catalog_entry =
	    con.context->catalog.GetEntry(*con.context, CatalogType::AGGREGATE_FUNCTION, DEFAULT_SCHEMA, name, false);
	assert(catalog_entry->type == CatalogType::AGGREGATE_FUNCTION);
	auto aggr_fun = (AggregateFunctionCatalogEntry *)catalog_entry;

	idx_t best_function = Function::BindFunction(aggr_fun->name, aggr_fun->functions, function_args);
	auto fun = aggr_fun->functions[best_function];
	return make_unique<BoundAggregateExpression>(GetInternalType(fun.return_type), fun, false);
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

	MyScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();

	auto &context = *con.context;

	con.context->catalog.CreateTableFunction(*con.context, &info);

	// use sql for everything
	auto result = con.Query("SELECT (some_int + 42) % 2, count(*) FROM my_scan() WHERE some_int BETWEEN 3 AND 7 group "
	                        "by some_int ORDER BY 1");
	result->Print();

	result = con.Query("EXPLAIN SELECT (some_int + 42) % 2, count(*) FROM my_scan() WHERE some_int BETWEEN 3 AND 7 "
	                   "group by some_int ORDER BY 1");
	result->Print();

	// fully manual with custom scan op

	/*
	 ORDER_BY
	    PROJECTION[%(+(some_int, 42), 2) count()]
	        HASH_GROUP_BY
	            FILTER[some_int<=7 some_int>=3]
	                TABLE_FUNCTION
	*/

	vector<TypeId> types{TypeId::INT32, TypeId::VARCHAR};

	auto bind_data = make_unique<MyScanFunctionData>();

	// TABLE_FUNCTION my_scan
	vector<unique_ptr<ParsedExpression>> children; // empty
	auto scan_function_catalog_entry =
	    con.context->catalog.GetEntry<TableFunctionCatalogEntry>(*con.context, DEFAULT_SCHEMA, "my_scan");
	vector<Value> parameters; // empty
	auto scan_function =
	    make_unique<PhysicalTableFunction>(types, scan_function_catalog_entry, move(bind_data), move(parameters));

	//  FILTER[some_int<=7 some_int>=3]
	vector<unique_ptr<Expression>> filter_expressions;

	auto lte_expr = make_unique_base<Expression, BoundComparisonExpression>(
	    ExpressionType::COMPARE_LESSTHANOREQUALTO,
	    make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0),
	    make_unique_base<Expression, BoundConstantExpression>(Value::INTEGER(7)));

	auto gte_expr = make_unique_base<Expression, BoundComparisonExpression>(
	    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	    make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0),
	    make_unique_base<Expression, BoundConstantExpression>(Value::INTEGER(3)));

	filter_expressions.push_back(move(lte_expr));
	filter_expressions.push_back(move(gte_expr));

	auto filter = make_unique<PhysicalFilter>(types, move(filter_expressions));
	filter->children.push_back(move(scan_function));

	// HASH_GROUP_BY some_int aggregating COUNT(*)
	vector<TypeId> aggr_types{TypeId::INT32, TypeId::INT64};
	vector<unique_ptr<Expression>> aggr_expressions;
	aggr_expressions.push_back(resolve_aggregate(con, "count", {}));

	vector<unique_ptr<Expression>> aggr_groups;
	aggr_groups.push_back(make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0));

	auto group_by = make_unique<PhysicalHashAggregate>(aggr_types, move(aggr_expressions), move(aggr_groups));
	group_by->children.push_back(move(filter));

	// PROJECTION[%(+(some_int, 42), 2) count()]
	auto add_expr = resolve_function(con, "+", {SQLTypeId::INTEGER, SQLTypeId::INTEGER});
	add_expr->children.push_back(make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0));
	add_expr->children.push_back(make_unique_base<Expression, BoundConstantExpression>(Value::INTEGER(42)));

	auto mod_expr = resolve_function(con, "%", {SQLTypeId::INTEGER, SQLTypeId::INTEGER});
	mod_expr->children.push_back(move(add_expr));
	mod_expr->children.push_back(make_unique_base<Expression, BoundConstantExpression>(Value::INTEGER(2)));

	vector<unique_ptr<Expression>> proj_expressions;
	proj_expressions.push_back(move(mod_expr));
	proj_expressions.push_back(make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT64, 1));
	auto projection = make_unique<PhysicalProjection>(aggr_types, move(proj_expressions));
	projection->children.push_back(move(group_by));

	// ORDER_BY 1
	BoundOrderByNode order_by;
	order_by.type = OrderType::ASCENDING;
	order_by.expression = make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0);

	vector<BoundOrderByNode> orders;
	orders.push_back(move(order_by));

	auto order = make_unique<PhysicalOrder>(aggr_types, move(orders));
	order->children.push_back(move(projection));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(order->types);
	auto state = order->GetOperatorState();

	do {
		order->GetChunk(*con.context, result_chunk, state.get());
		result_chunk.Print();
	} while (result_chunk.size() > 0);
}
