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
	PragmaCollations::RegisterFunction(*this);
	PragmaTableInfo::RegisterFunction(*this);
	SQLiteMaster::RegisterFunction(*this);

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
