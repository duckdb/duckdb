#include <string>
#include <utility>

#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/insert_query_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref.hpp"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformValuesList(duckdb_libpgquery::PGList *list) {
	auto result = make_uniq<ExpressionListRef>();
	for (auto value_list = list->head; value_list != nullptr; value_list = value_list->next) {
		auto target = PGPointerCast<duckdb_libpgquery::PGList>(value_list->data.ptr_value);

		vector<unique_ptr<ParsedExpression>> insert_values;
		TransformExpressionList(*target, insert_values);
		if (!result->values.empty()) {
			if (result->values[0].size() != insert_values.size()) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
		result->values.push_back(std::move(insert_values));
	}
	result->alias = "valueslist";
	return std::move(result);
}

vector<string> Transformer::TransformInsertColumns(duckdb_libpgquery::PGList &cols) {
	vector<string> result;
	for (auto c = cols.head; c != nullptr; c = lnext(c)) {
		auto target = PGPointerCast<duckdb_libpgquery::PGResTarget>(c->data.ptr_value);
		result.emplace_back(target->name);
	}
	return result;
}

InsertColumnOrder Transformer::TransformColumnOrder(duckdb_libpgquery::PGInsertColumnOrder insert_column_order) {
	switch (insert_column_order) {
	case duckdb_libpgquery::PG_INSERT_BY_POSITION:
		return InsertColumnOrder::INSERT_BY_POSITION;
	case duckdb_libpgquery::PG_INSERT_BY_NAME:
		return InsertColumnOrder::INSERT_BY_NAME;
	default:
		throw InternalException("Unrecognized insert column order in TransformInsert");
	}
}

unique_ptr<InsertStatement> Transformer::TransformInsert(duckdb_libpgquery::PGInsertStmt &stmt) {
	auto result = make_uniq<InsertStatement>();
	auto &node = *result->node;
	if (stmt.withClause) {
		TransformCTE(*PGPointerCast<duckdb_libpgquery::PGWithClause>(stmt.withClause), node.cte_map);
	}

	// first check if there are any columns specified
	if (stmt.cols) {
		node.columns = TransformInsertColumns(*stmt.cols);
	}

	// Grab and transform the returning columns from the parser.
	if (stmt.returningList) {
		TransformExpressionList(*stmt.returningList, node.returning_list);
	}
	if (stmt.selectStmt) {
		node.select_statement = TransformSelectStmt(*stmt.selectStmt, false);
	} else {
		node.default_values = true;
	}

	auto qname = TransformQualifiedName(*stmt.relation);
	node.table = qname.name;
	node.schema = qname.schema;

	if (stmt.onConflictClause) {
		if (stmt.onConflictAlias != duckdb_libpgquery::PG_ONCONFLICT_ALIAS_NONE) {
			// OR REPLACE | OR IGNORE are shorthands for the ON CONFLICT clause
			throw ParserException("You can not provide both OR REPLACE|IGNORE and an ON CONFLICT clause, please remove "
			                      "the first if you want to have more granual control");
		}
		node.on_conflict_info = TransformOnConflictClause(stmt.onConflictClause, node.schema);
		node.table_ref = TransformRangeVar(*stmt.relation);
	}
	if (stmt.onConflictAlias != duckdb_libpgquery::PG_ONCONFLICT_ALIAS_NONE) {
		D_ASSERT(!stmt.onConflictClause);
		node.on_conflict_info = DummyOnConflictClause(stmt.onConflictAlias, node.schema);
		node.table_ref = TransformRangeVar(*stmt.relation);
	}
	node.column_order = TransformColumnOrder(stmt.insert_column_order);
	node.catalog = qname.catalog;
	return result;
}

} // namespace duckdb
