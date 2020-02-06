#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/operator/list.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"

using namespace duckdb;
using namespace std;



TEST_CASE("Test filter and projection of nested struct", "[nested]") {
	DuckDB db(nullptr);
	Connection con(db);
	con.DisableProfiling();

// This is a dummy entry
	auto agg = AggregateFunction("list", {SQLType::INTEGER}, SQLType::FLOAT, nullptr, nullptr,
			nullptr, nullptr, nullptr);
	CreateAggregateFunctionInfo agg_info(agg);

	con.context->transaction.SetAutoCommit(false);
	con.context->transaction.BeginTransaction();
	auto &trans = con.context->transaction.ActiveTransaction();
	con.context->catalog.CreateFunction(trans, &agg_info);


	con.Query("CREATE TABLE list_data (g INTEGER, e INTEGER)");
	con.Query("INSERT INTO list_data VALUES (1, 1), (1, 2), (2, 3), (2, 4), (2, 5), (3, 6), (5, NULL)");


	auto result = con.Query("SELECT g, STRUCT_PACK(gg := g, ee := e) FROM list_data ");
	result->Print();

	result = con.Query("SELECT g, STRUCT_PACK(gg := g, ee := e, ff := e) FROM list_data ");
	result->Print();

	result = con.Query("SELECT e, STRUCT_PACK(xx := e) FROM list_data ");
	result->Print();

	result = con.Query("SELECT e = STRUCT_EXTRACT(STRUCT_PACK(xx := e, yy := g), 'xx') as ee FROM list_data ORDER BY e");
	result->Print();


	 result = con.Query("SELECT g, LIST(e) from list_data GROUP BY g");
	result->Print();

	result = con.Query("SELECT g, LIST(CAST(e AS VARCHAR)) from list_data GROUP BY g");
	result->Print();

	result = con.Query("SELECT g, LIST(e/2.0) from list_data GROUP BY g");
	result->Print();




//	// TODO
//	result = con.Query("SELECT g, LIST(STRUCT_PACK(xx := e)) FROM list_data GROUP BY g");
//	result->Print();
//
//	// TODO
//	result = con.Query("SELECT STRUCT_PACK(a := 42, b := 43) ");
//	result->Print();


	// TODO ?

//
//
//
//	scalar modify params, evil but ok
//	aggrs add function bind, pass binddata to callbacks, add cleanup function
//
//
//
//
//	create table a (i integer, j list<integer>, k list<integer>)
//
//
//	select * from  UNLIST(a, 'j')
//
//	LIST_EXTRACT(a, 42)
//	a[42]
//
//
//
//	i integer, j integer, k list<integer>, OFFSETS
//
//
//
//
//
//	STRUCT_EXTRACT(a, 'b')

}
