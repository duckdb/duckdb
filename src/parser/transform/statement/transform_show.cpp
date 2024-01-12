#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/showref.hpp"

namespace duckdb {

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

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	auto showref = make_uniq<ShowRef>();
	showref->table_name = std::move(name);
	select_node->from_table = std::move(showref);

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(select_node);
	return result;
}

} // namespace duckdb
