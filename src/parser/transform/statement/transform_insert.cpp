#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {
using namespace std;
using namespace duckdb_libpgquery;

unique_ptr<TableRef> Transformer::TransformValuesList(PGList *list) {
	auto result = make_unique<ExpressionListRef>();
	for (auto value_list = list->head; value_list != NULL; value_list = value_list->next) {
		auto target = (PGList *)(value_list->data.ptr_value);

		vector<unique_ptr<ParsedExpression>> insert_values;
		if (!TransformExpressionList(target, insert_values)) {
			throw ParserException("Could not parse expression list!");
		}
		if (result->values.size() > 0) {
			if (result->values[0].size() != insert_values.size()) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
		result->values.push_back(move(insert_values));
	}
	result->alias = "valueslist";
	return move(result);
}

unique_ptr<InsertStatement> Transformer::TransformInsert(PGNode *node) {
	auto stmt = reinterpret_cast<PGInsertStmt *>(node);
	assert(stmt);

	auto result = make_unique<InsertStatement>();

	// first check if there are any columns specified
	if (stmt->cols) {
		for (auto c = stmt->cols->head; c != NULL; c = lnext(c)) {
			auto target = (PGResTarget *)(c->data.ptr_value);
			result->columns.push_back(string(target->name));
		}
	}
	result->select_statement = TransformSelect(stmt->selectStmt);

	auto qname = TransformQualifiedName(stmt->relation);
	result->table = qname.name;
	result->schema = qname.schema;
	return result;
}

} // namespace duckdb
