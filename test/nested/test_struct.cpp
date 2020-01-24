#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace std;

struct MyScanFunctionData : public TableFunctionData {
	MyScanFunctionData() : nrow(100) {
	}

	size_t nrow;
};

FunctionData *my_scan_function_init(ClientContext &context) {
	// initialize the function data structure
	return new MyScanFunctionData();
}

void my_scan_function(ClientContext &context, DataChunk &input, DataChunk &output, FunctionData *dataptr) {
	auto &data = *((MyScanFunctionData *)dataptr);

	if (data.nrow < 1) {
		return;
	}

	// generate data for two output columns
	size_t this_rows = std::min(data.nrow, (size_t)1024);
	data.nrow -= this_rows;

	auto int_data = (int32_t *)output.data[0].GetData();
	for (size_t row = 0; row < this_rows; row++) {
		int_data[row] = row % 10;
	}
	output.data[0].count = this_rows;

	// TODO: the nested stuff should probably live in the data chunks's data area as well (?)
	auto &v = output.data[1];
	auto cv1 = make_unique<Vector>(TypeId::INT32);
	cv1->Initialize(TypeId::INT32, true);
	auto cv2 = make_unique<Vector>(TypeId::DOUBLE);
	cv2->Initialize(TypeId::DOUBLE, true);

	auto cv1_data = (int32_t *)cv1->GetData();
	auto cv2_data = (double *)cv2->GetData();

	for (size_t row = 0; row < this_rows; row++) {
		// need to construct struct stuff here

		cv1_data[row] = 1;
		cv2_data[row] = 2;
	}

	cv1->count = this_rows;
	cv2->count = this_rows;

	// TODO we need to verify the schema here
	v.children.push_back(pair<string, unique_ptr<Vector>>("first", move(cv1)));
	v.children.push_back(pair<string, unique_ptr<Vector>>("second", move(cv2)));

	output.data[1].nullmask.none();
	output.data[1].count = this_rows;
}

class MyScanFunction : public TableFunction {
public:
	MyScanFunction() : TableFunction(MyScanConstruct()){};

private:
	TableFunction MyScanConstruct() { // TODO is this the simplest way of doing this?
		SQLType struct_type(SQLTypeId::STRUCT);
		struct_type.struct_type.push_back(pair<string, SQLType>("first", SQLType::INTEGER));
		struct_type.struct_type.push_back(pair<string, SQLType>("second", SQLType::DOUBLE));
		return TableFunction("my_scan", {}, {SQLType::INTEGER, struct_type}, {"some_int", "some_struct"},
		                     my_scan_function_init, my_scan_function, nullptr);
	}
};

unique_ptr<BoundFunctionExpression> resolve_function(Connection &con, string name, vector<SQLType> function_args,
                                                     bool is_operator = true) {
	auto catalog_entry =
	    con.context->catalog.GetFunction(con.context->transaction.ActiveTransaction(), DEFAULT_SCHEMA, name, false);
	assert(catalog_entry->type == CatalogType::SCALAR_FUNCTION);
	auto scalar_fun = (ScalarFunctionCatalogEntry *)catalog_entry;

	index_t best_function = Function::BindFunction(scalar_fun->name, scalar_fun->functions, function_args);
	auto fun = scalar_fun->functions[best_function];

	return make_unique<BoundFunctionExpression>(GetInternalType(fun.return_type), fun, is_operator);
}

TEST_CASE("Test filter and projection of nested struct", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);

	MyScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();
	auto &trans = con.context->transaction.ActiveTransaction();
	con.context->catalog.CreateTableFunction(trans, &info);

	auto result = con.Query("SELECT * FROM my_scan() where some_int > 7");
	result->Print();



	vector<TypeId> types{TypeId::INT32, TypeId::STRUCT};

	// TABLE_FUNCTION my_scan
	vector<unique_ptr<ParsedExpression>> children; // empty
	FunctionExpression fun_expr(DEFAULT_SCHEMA, "my_scan", children);
	auto scan_function_catalog_entry = con.context->catalog.GetTableFunction(trans, &fun_expr);
	vector<unique_ptr<Expression>> parameters; // empty
	auto scan_function = make_unique<PhysicalTableFunction>(types, scan_function_catalog_entry, move(parameters));

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

	// PROJECTION[some_int, EXTRACT_STRUCT_MEMBER()]

	vector<unique_ptr<Expression>> proj_expressions;
	proj_expressions.push_back(make_unique_base<Expression, BoundReferenceExpression>(TypeId::INT32, 0));
	proj_expressions.push_back(make_unique_base<Expression, BoundReferenceExpression>(TypeId::STRUCT, 1));
	auto projection = make_unique<PhysicalProjection>(types, move(proj_expressions));
	//projection->children.push_back(move(filter));

	// execute!
	DataChunk result_chunk;
	result_chunk.Initialize(filter->types);
	auto state = filter->GetOperatorState();

	do {
		filter->GetChunk(*con.context, result_chunk, state.get());
		result_chunk.Print();
	} while (result_chunk.size() > 0);



}
