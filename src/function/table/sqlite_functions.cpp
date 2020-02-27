#include "duckdb/function/table/sqlite_functions.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/catalog/catalog.hpp"

using namespace std;

namespace duckdb {

void BuiltinFunctions::RegisterSQLiteFunctions() {
	AddFunction(TableFunction(
	    "sqlite_master", {}, {SQLType::VARCHAR, SQLType::VARCHAR, SQLType::VARCHAR, SQLType::INTEGER, SQLType::VARCHAR},
	    {"type", "name", "tbl_name", "rootpage", "sql"}, sqlite_master_init, sqlite_master, nullptr));

	AddFunction(TableFunction("pragma_table_info", {SQLType::VARCHAR},
	                          {SQLType::INTEGER, SQLType::VARCHAR, SQLType::VARCHAR, SQLType(SQLTypeId::BOOLEAN),
	                           SQLType::VARCHAR, SQLType(SQLTypeId::BOOLEAN)},
	                          {"cid", "name", "type", "notnull", "dflt_value", "pk"}, pragma_table_info_init,
	                          pragma_table_info, nullptr));

	CreateViewInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.view_name = "sqlite_master";
	info.on_conflict = OnCreateConflict::REPLACE;

	auto select = make_unique<SelectNode>();
	select->select_list.push_back(make_unique<StarExpression>());
	vector<unique_ptr<ParsedExpression>> children;

	auto function = make_unique<FunctionExpression>(DEFAULT_SCHEMA, "sqlite_master", children);
	auto function_expr = make_unique<TableFunctionRef>();
	function_expr->function = move(function);
	select->from_table = move(function_expr);
	info.query = move(select);
//	catalog.CreateView(transaction, &info);
}

} // namespace duckdb
