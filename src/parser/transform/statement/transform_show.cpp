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
	string name = stmt.name;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto showref = make_uniq<ShowRef>();
	showref->table_name = std::move(name);
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
