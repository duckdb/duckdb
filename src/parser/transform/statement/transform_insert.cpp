#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

using namespace duckdb;
using namespace std;

void Transformer::TransformValuesList(postgres::List *list, vector<vector<unique_ptr<ParsedExpression>>> &values) {
	for (auto value_list = list->head; value_list != NULL; value_list = value_list->next) {
		auto target = (postgres::List *)(value_list->data.ptr_value);

		vector<unique_ptr<ParsedExpression>> insert_values;
		if (!TransformExpressionList(target, insert_values)) {
			throw ParserException("Could not parse expression list!");
		}
		if (values.size() > 0) {
			if (values[0].size() != insert_values.size()) {
				throw ParserException("VALUES lists must all be the same length");
			}
		}
		values.push_back(move(insert_values));
	}
}

unique_ptr<InsertStatement> Transformer::TransformInsert(postgres::Node *node) {
	auto stmt = reinterpret_cast<postgres::InsertStmt *>(node);
	assert(stmt);

	auto result = make_unique<InsertStatement>();

	// first check if there are any columns specified
	if (stmt->cols) {
		for (auto c = stmt->cols->head; c != NULL; c = lnext(c)) {
			auto target = (postgres::ResTarget *)(c->data.ptr_value);
			result->columns.push_back(string(target->name));
		}
	}

	auto select_stmt = reinterpret_cast<postgres::SelectStmt *>(stmt->selectStmt);
	if (!select_stmt->valuesLists) {
		// insert from select statement
		result->select_statement = TransformSelect(stmt->selectStmt);
	} else {
		// transform the value list
		TransformValuesList(select_stmt->valuesLists, result->values);
	}

	auto ref = TransformRangeVar(stmt->relation);
	auto &table = *reinterpret_cast<BaseTableRef *>(ref.get());
	result->table = table.table_name;
	result->schema = table.schema_name;
	return result;
}
