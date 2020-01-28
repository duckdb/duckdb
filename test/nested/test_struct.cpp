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
		cv1_data[row] = row;
		cv2_data[row] = row;
		output.data[1].nullmask[row] = row % 2 == 0;
	}

	cv1->count = this_rows;
	cv2->count = this_rows;
	cv1->vector_type = VectorType::FLAT_VECTOR;
	cv2->vector_type = VectorType::FLAT_VECTOR;

	// TODO we need to verify the schema here
	v.children.push_back(pair<string, unique_ptr<Vector>>("first", move(cv1)));
	v.children.push_back(pair<string, unique_ptr<Vector>>("second", move(cv2)));

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
	con.DisableProfiling();

	MyScanFunction scan_fun;
	CreateTableFunctionInfo info(scan_fun);
	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();
	auto &trans = con.context->transaction.ActiveTransaction();
	con.context->catalog.CreateTableFunction(trans, &info);

	auto result = con.Query("SELECT some_int, some_struct FROM my_scan() WHERE some_int > 7 LIMIT 10 ");
	result->Print();

	// TODO sorting
	// TODO hash tables
	// TODO ?
}
