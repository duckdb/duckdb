#include "parser/statement/insert_statement.hpp"
#include "planner/binder.hpp"
#include "planner/statement/bound_insert_statement.hpp"
#include "planner/expression_binder/where_binder.hpp"

#include "main/client_context.hpp"
#include "main/database.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(InsertStatement &stmt) {
	auto result = make_unique<BoundInsertStatement>();
	result->table = context.db.catalog.GetTable(context.ActiveTransaction(), stmt.schema, stmt.table);

	if (stmt.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		map<string, int> column_name_map;
		for (size_t i = 0; i < stmt.columns.size(); i++) {
			column_name_map[stmt.columns[i]] = i;
		}
		for (size_t i = 0; i < result->table->columns.size(); i++) {
			auto &col = result->table->columns[i];
			auto entry = column_name_map.find(col.name);
			if (entry == column_name_map.end()) {
				// column not specified, set index to -1
				result->column_index_map.push_back(-1);
			} else {
				// column was specified, set to the index
				result->column_index_map.push_back(entry->second);
			}
		}
	}

	if (stmt.select_statement) {
		result->select_statement = unique_ptr_cast<BoundSQLStatement, BoundSelectStatement>(Bind(*stmt.select_statement));
	} else {
		int expected_columns = stmt.columns.size() == 0 ? result->table->columns.size() : stmt.columns.size();
		// visit the expressions
		for (auto &expression_list : stmt.values) {
			if (expression_list.size() != expected_columns) {
				string msg =
				    StringUtil::Format(stmt.columns.size() == 0 ? "table %s has %d columns but %d values were supplied"
				                                                : "Column name/value mismatch for insert on %s: "
				                                                  "expected %d columns but %d values were supplied",
				                       result->table->name.c_str(), expected_columns, expression_list.size());
				throw BinderException(msg);
			}
			vector<unique_ptr<Expression>> list;
			for (auto &expression : expression_list) {
				WhereBinder binder(*this, context);
				list.push_back(binder.Bind(expression));
			}
			result->values.push_back(move(list));
		}
	}
	return move(result);
}
