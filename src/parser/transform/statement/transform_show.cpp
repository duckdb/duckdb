#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/planner/binding_alias.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformShow(duckdb_libpgquery::PGVariableShowStmt &stmt) {
	// create the query that holds the show statement
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto showref = make_uniq<ShowRef>();
	if (stmt.set) {
		if (std::string(stmt.set) == "__show_tables_from_database") {
			showref->show_type = ShowType::SHOW_FROM;
			auto qualified_name = TransformQualifiedName(*stmt.relation);
			if (!IsInvalidCatalog(qualified_name.catalog)) {
				throw ParserException("Expected \"SHOW TABLES FROM database\", \"SHOW TABLES FROM schema\", or "
				                      "\"SHOW TABLES FROM database.schema\"");
			}
			if (qualified_name.schema.empty()) {
				showref->schema_name = qualified_name.name;
			} else {
				showref->catalog_name = qualified_name.schema;
				showref->schema_name = qualified_name.name;
			}
		} else if (std::string(stmt.set) == "__show_all_settings") {
			// PG "SHOW ALL" -> SELECT name, setting, description FROM pg_settings
			auto result = make_uniq<SelectNode>();
			result->select_list.emplace_back(make_uniq<ColumnRefExpression>("name"));
			result->select_list.emplace_back(make_uniq<ColumnRefExpression>("setting"));
			result->select_list.emplace_back(
			    make_uniq<ColumnRefExpression>("short_desc", BindingAlias {"description"}));
			auto tableref = make_uniq<BaseTableRef>();
			tableref->table_name = "pg_settings";
			result->from_table = std::move(tableref);
			return std::move(result);
		} else {
			// describing a set (e.g. SHOW ALL TABLES) - push it in the table name
			showref->table_name = stmt.set;
			showref->show_type = ShowType::SHOW_UNQUALIFIED;
		}
	} else if (!stmt.relation->schemaname) {
		// describing an unqualified relation - check if this is a "special" relation
		string table_name = StringUtil::Lower(stmt.relation->relname);
		if (table_name == "databases" || table_name == "schemas" || table_name == "tables" ||
		    table_name == "variables") {
			showref->table_name = "\"" + std::move(table_name) + "\"";
			showref->show_type = ShowType::SHOW_UNQUALIFIED;
		}
	}
	if (showref->table_name.empty() && showref->show_type != ShowType::SHOW_FROM) {
		if (!stmt.relation->schemaname) {
			// PG "SHOW varname" -> SELECT current_setting('varname') AS "varname"
			// Return directly, bypassing ShowRef
			string var_name = stmt.relation->relname;
			auto result = make_uniq<SelectNode>();
			vector<unique_ptr<ParsedExpression>> args;
			args.push_back(make_uniq<ConstantExpression>(Value(var_name)));
			auto func_expr = make_uniq<FunctionExpression>("current_setting", std::move(args));
			func_expr->alias = var_name;
			result->select_list.push_back(std::move(func_expr));
			result->from_table = make_uniq<EmptyTableRef>();
			return std::move(result);
		}
		// describing a qualified relation (schema.table)
		auto show_select_node = make_uniq<SelectNode>();
		show_select_node->select_list.push_back(make_uniq<StarExpression>());
		auto tableref = TransformRangeVar(*stmt.relation);
		show_select_node->from_table = std::move(tableref);
		showref->query = std::move(show_select_node);
	}

	// If the show type is set to default, check if summary
	if (showref->show_type == ShowType::DESCRIBE) {
		showref->show_type = stmt.is_summary ? ShowType::SUMMARY : ShowType::DESCRIBE;
	}
	select_node->from_table = std::move(showref);
	return std::move(select_node);
}

unique_ptr<SelectStatement> Transformer::TransformShowStmt(duckdb_libpgquery::PGVariableShowStmt &stmt) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformShow(stmt);
	return result;
}

} // namespace duckdb
