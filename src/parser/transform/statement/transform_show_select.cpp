#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

unique_ptr<QueryNode> Transformer::TransformShowSelect(duckdb_libpgquery::PGVariableShowSelectStmt &stmt) {
	// we capture the select statement of SHOW
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());

	auto show_ref = make_uniq<ShowRef>();
	show_ref->show_type = stmt.is_summary ? ShowType::SUMMARY : ShowType::DESCRIBE;
	show_ref->query = TransformSelectNode(*stmt.stmt);
	select_node->from_table = std::move(show_ref);
	return std::move(select_node);
}

unique_ptr<SelectStatement> Transformer::TransformShowSelectStmt(duckdb_libpgquery::PGVariableShowSelectStmt &stmt) {
	auto result = make_uniq<SelectStatement>();
	result->node = TransformShowSelect(stmt);
	return result;
}

} // namespace duckdb
