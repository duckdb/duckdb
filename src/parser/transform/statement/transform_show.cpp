#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

static void TransformShowName(unique_ptr<PragmaStatement> &result, const string &name) {
	auto &info = *result->info;

	if (name == "\"tables\"") {
		// show all tables
		info.name = "show_tables";
	} else if (name == "__show_tables_expanded") {
		info.name = "show_tables_expanded";
	} else {
		// show one specific table
		info.name = "show";
		info.parameters.emplace_back(name);
	}
}

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
		auto qualified_name = QualifiedName::Parse(stmt->name);
		basetable->schema_name = qualified_name.schema;
		basetable->table_name = qualified_name.name;
		select->from_table = move(basetable);

		info.query = move(select);
		return move(result);
	}

	auto result = make_unique<PragmaStatement>();

	auto show_name = stmt->name;
	TransformShowName(result, show_name);
	return move(result);
}

} // namespace duckdb
