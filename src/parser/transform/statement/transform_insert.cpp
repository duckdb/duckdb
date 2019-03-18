#include "parser/statement/insert_statement.hpp"
#include "parser/tableref/basetableref.hpp"
#include "parser/transformer.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

unique_ptr<InsertStatement> Transformer::TransformInsert(Node *node) {
	InsertStmt *stmt = reinterpret_cast<InsertStmt *>(node);
	assert(stmt);

	auto result = make_unique<InsertStatement>();

	// first check if there are any columns specified
	if (stmt->cols) {
		for (ListCell *c = stmt->cols->head; c != NULL; c = lnext(c)) {
			ResTarget *target = (ResTarget *)(c->data.ptr_value);
			result->columns.push_back(string(target->name));
		}
	}

	auto select_stmt = reinterpret_cast<SelectStmt *>(stmt->selectStmt);
	if (!select_stmt->valuesLists) {
		// insert from select statement
		result->select_statement = TransformSelect(stmt->selectStmt);
	} else {
		// transform the insert list
		auto list = select_stmt->valuesLists;
		for (auto value_list = list->head; value_list != NULL; value_list = value_list->next) {
			List *target = (List *)(value_list->data.ptr_value);

			vector<unique_ptr<ParsedExpression>> insert_values;
			if (!TransformExpressionList(target, insert_values)) {
				throw ParserException("Could not parse expression list!");
			}
			if (result->values.size() > 0) {
				if (result->values[0].size() != insert_values.size()) {
					throw ParserException("Insert VALUES lists must all be the same length");
				}
			}
			result->values.push_back(move(insert_values));
		}
	}

	auto ref = TransformRangeVar(stmt->relation);
	auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
	result->table = table.table_name;
	result->schema = table.schema_name;
	return result;
}
