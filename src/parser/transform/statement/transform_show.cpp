#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformShow(duckdb_libpgquery::PGVariableShowStmt &stmt) {
	// create the query that holds the show statement
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto showref = make_uniq<ShowRef>();
	if (stmt.set) {
		// describing a set (e.g. SHOW ALL TABLES) - push it in the table name
		showref->table_name = stmt.set;
	} else if (!stmt.relation->schemaname) {
		// describing an unqualified relation - check if this is a "special" relation
		string table_name = StringUtil::Lower(stmt.relation->relname);
		if (table_name == "databases" || table_name == "tables" || table_name == "variables") {
			showref->table_name = "\"" + std::move(table_name) + "\"";
		}
	}
	if (showref->table_name.empty()) {
		// describing a single relation
		// wrap the relation in a "SELECT * FROM [table_name]" query
		auto show_select_node = make_uniq<SelectNode>();
		show_select_node->select_list.push_back(make_uniq<StarExpression>());
		auto tableref = TransformRangeVar(*stmt.relation);
		show_select_node->from_table = std::move(tableref);
		showref->query = std::move(show_select_node);
	}

	showref->show_type = stmt.is_summary ? ShowType::SUMMARY : ShowType::DESCRIBE;
	select_node->from_table = std::move(showref);
	return std::move(select_node);
}

unique_ptr<SelectStatement> Transformer::TransformShowStmt(duckdb_libpgquery::PGVariableShowStmt &stmt) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformShow(stmt);
	return result;
}

} // namespace duckdb
