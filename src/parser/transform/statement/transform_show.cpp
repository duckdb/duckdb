#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

unique_ptr<SQLStatement> Transformer::TransformShow(duckdb_libpgquery::PGNode *node) {
	// we transform SHOW x into PRAGMA SHOW('x')

	auto stmt = reinterpret_cast<duckdb_libpgquery::PGVariableShowStmt *>(node);
	if (stmt->is_summary) {
		auto result = make_unique<ShowStatement>();
		auto &info = *result->info;
		info.is_summary = stmt->is_summary;

		auto select = make_unique<SelectNode>();
		select->select_list.push_back(make_unique<StarExpression>());
		auto basetable = make_unique<BaseTableRef>();
		basetable->table_name = stmt->name;
		select->from_table = move(basetable);

		info.query = move(select);
		return result;
	}

	auto result = make_unique<PragmaStatement>();
	auto &info = *result->info;

	if (string(stmt->name) == "tables") {
		// show all tables
		info.name = "show_tables";
	} else {
		// show one specific table
		info.name = "show";
		info.parameters.emplace_back(stmt->name);
	}

	return result;
}

} // namespace duckdb
