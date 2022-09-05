#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<TableRef> Transformer::TransformValuesList(duckdb_libpgquery::PGList *list) {
	auto result = make_unique<ExpressionListRef>();
	for (auto value_list = list->head; value_list != nullptr; value_list = value_list->next) {
		auto target = (duckdb_libpgquery::PGList *)(value_list->data.ptr_value);

		vector<unique_ptr<ParsedExpression>> insert_values;
		TransformExpressionList(*target, insert_values);
		if (!result->values.empty()) {
			if (result->values[0].size() != insert_values.size()) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
		result->values.push_back(move(insert_values));
	}
	result->alias = "valueslist";
	return move(result);
}

unique_ptr<InsertStatement> Transformer::TransformInsert(duckdb_libpgquery::PGNode *node) {
	auto stmt = reinterpret_cast<duckdb_libpgquery::PGInsertStmt *>(node);
	D_ASSERT(stmt);
	if (stmt->onConflictClause && stmt->onConflictClause->action != duckdb_libpgquery::PG_ONCONFLICT_NONE) {
		throw ParserException("ON CONFLICT IGNORE/UPDATE clauses are not supported");
	}
	if (!stmt->selectStmt) {
		throw ParserException("DEFAULT VALUES clause is not supported!");
	}

	auto result = make_unique<InsertStatement>();
	if (stmt->withClause) {
		TransformCTE(reinterpret_cast<duckdb_libpgquery::PGWithClause *>(stmt->withClause), result->cte_map);
	}

	// first check if there are any columns specified
	if (stmt->cols) {
		for (auto c = stmt->cols->head; c != nullptr; c = lnext(c)) {
			auto target = (duckdb_libpgquery::PGResTarget *)(c->data.ptr_value);
			result->columns.emplace_back(target->name);
		}
	}

	// Grab and transform the returning columns from the parser.
	if (stmt->returningList) {
		Transformer::TransformExpressionList(*(stmt->returningList), result->returning_list);
	}
	result->select_statement = TransformSelect(stmt->selectStmt, false);

	auto qname = TransformQualifiedName(stmt->relation);
	result->table = qname.name;
	result->schema = qname.schema;
	return result;
}

} // namespace duckdb
