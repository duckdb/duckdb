#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

//static void TransformShowName(unique_ptr<PragmaStatement> &result, const string &name) {
//	auto &info = *result->info;
//	auto lname = StringUtil::Lower(name);
//
//	if (lname == "\"databases\"") {
//		info.name = "show_databases";
//	} else if (lname == "\"tables\"") {
//		// show all tables
//		info.name = "show_tables";
//	} else if (lname == "__show_tables_expanded") {
//		info.name = "show_tables_expanded";
//	} else {
//		// show one specific table
//		info.name = "show";
//		info.parameters.emplace_back(make_uniq<ConstantExpression>(Value(name)));
//	}
//}

unique_ptr<TableFunctionRef> CreateTableFunctionRef(string name, vector<Value> parameters) {
	// show one specific table
	vector<unique_ptr<ParsedExpression>> children;
	for(auto &param : parameters) {
		children.push_back(make_uniq<ConstantExpression>(std::move(param)));
	}
	auto function = make_uniq<FunctionExpression>(std::move(name), std::move(children));

	auto table_function = make_uniq<TableFunctionRef>();
	table_function->function = std::move(function);
	return table_function;
}

unique_ptr<TableFunctionRef> CreateTableFunctionRef(string name) {
	vector<Value> parameters;
	return CreateTableFunctionRef(std::move(name), std::move(parameters));
}

unique_ptr<ResultModifier> CreateOrderBy(vector<string> order_by_columns) {
	auto order_modifier = make_uniq<OrderModifier>();
	for(auto &col : order_by_columns) {
		auto expr = make_uniq<ColumnRefExpression>(std::move(col));
		OrderByNode node(OrderType::ORDER_DEFAULT, OrderByNullType::ORDER_DEFAULT, std::move(expr));
		order_modifier->orders.push_back(std::move(node));
	}
	return std::move(order_modifier);
}

void ShowDatabases(SelectNode &result) {
	result.select_list.push_back(make_uniq<ColumnRefExpression>("database_name"));
	result.from_table = CreateTableFunctionRef(("duckdb_databases"));
	auto internal_ref = make_uniq<ColumnRefExpression>("internal");
	result.where_clause = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(internal_ref));
	result.modifiers.push_back(CreateOrderBy(vector<string> { "database_name" }));
}

void ShowSingleTable(string name, SelectNode &result) {
	// show one specific table
	vector<Value> parameters;
	parameters.push_back(Value(std::move(name)));

	result.select_list.push_back(make_uniq<StarExpression>());
	result.from_table = CreateTableFunctionRef("pragma_show", std::move(parameters));
}

//	return "SELECT database_name FROM duckdb_databases() WHERE NOT internal ORDER BY database_name;";

unique_ptr<SelectStatement> Transformer::TransformShow(duckdb_libpgquery::PGVariableShowStmt &stmt) {
	// we transform SHOW x into PRAGMA SHOW('x')
	if (stmt.is_summary) {
		throw InternalException("FIXME: SHOW");
//		auto result = make_uniq<ShowStatement>();
//		auto &info = *result->info;
//		info.is_summary = stmt.is_summary;
//
//		auto select = make_uniq<SelectNode>();
//		select->select_list.push_back(make_uniq<StarExpression>());
//		auto basetable = make_uniq<BaseTableRef>();
//		auto qualified_name = QualifiedName::Parse(stmt.name);
//		basetable->schema_name = qualified_name.schema;
//		basetable->table_name = qualified_name.name;
//		select->from_table = std::move(basetable);
//
//		info.query = std::move(select);
//		return std::move(result);
	}

	string name = stmt.name;
	auto lname = StringUtil::Lower(name);

	auto select_node = make_uniq<SelectNode>();

	if (lname == "\"databases\"") {
		ShowDatabases(*select_node);
	} else if (lname == "\"tables\"") {
		throw InternalException("FIXME: SHOW TABLES");
	} else if (lname == "__show_tables_expanded") {
		throw InternalException("FIXME: SHOW ALL TABLES");
	} else {
		ShowSingleTable(std::move(name), *select_node);
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(select_node);
	return result;
}

} // namespace duckdb
