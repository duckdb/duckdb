#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/insert_statement.hpp"
#include "planner/binder.hpp"
#include "planner/expression_binder/insert_binder.hpp"
#include "planner/statement/bound_insert_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundSQLStatement> Binder::Bind(InsertStatement &stmt) {
	auto result = make_unique<BoundInsertStatement>();
	auto table = context.catalog.GetTable(context.ActiveTransaction(), stmt.schema, stmt.table);
	result->table = table;

	vector<uint32_t> named_column_map;
	if (stmt.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		unordered_map<string, uint32_t> column_name_map;
		for (uint64_t i = 0; i < stmt.columns.size(); i++) {
			assert(i <= numeric_limits<uint32_t>::max());

			column_name_map[stmt.columns[i]] = (uint32_t)i;
			auto entry = table->name_map.find(stmt.columns[i]);
			if (entry == table->name_map.end()) {
				throw BinderException("Column %s not found in table %s", stmt.columns[i].c_str(), table->name.c_str());
			}
			if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			result->expected_types.push_back(table->columns[entry->second].type);
			assert(entry->second <= numeric_limits<uint32_t>::max());
			named_column_map.push_back((uint32_t)entry->second);
		}
		for (uint64_t i = 0; i < result->table->columns.size(); i++) {
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
	} else {
		for (uint64_t i = 0; i < result->table->columns.size(); i++) {
			result->expected_types.push_back(table->columns[i].type);
		}
	}

	uint64_t expected_columns = stmt.columns.size() == 0 ? result->table->columns.size() : stmt.columns.size();
	if (stmt.select_statement) {
		result->select_statement =
		    unique_ptr_cast<BoundSQLStatement, BoundSelectStatement>(Bind(*stmt.select_statement));
		if (result->select_statement->node->types.size() != expected_columns) {
			string msg = StringUtil::Format(
			    stmt.columns.size() == 0 ? "table %s has %d columns but %d values were supplied"
			                             : "Column name/value mismatch for insert on %s: "
			                               "expected %d columns but %d values were supplied",
			    result->table->name.c_str(), (int)expected_columns, (int)result->select_statement->node->types.size());
			throw BinderException(msg);
		}
	} else {
		// visit the expressions
		InsertBinder binder(*this, context);
		for (auto &expression_list : stmt.values) {
			if (expression_list.size() != expected_columns) {
				string msg =
				    StringUtil::Format(stmt.columns.size() == 0 ? "table %s has %d columns but %d values were supplied"
				                                                : "Column name/value mismatch for insert on %s: "
				                                                  "expected %d columns but %d values were supplied",
				                       result->table->name.c_str(), (int)expected_columns, (int)expression_list.size());
				throw BinderException(msg);
			}
			vector<unique_ptr<Expression>> list;

			for (uint64_t col_idx = 0; col_idx < expression_list.size(); col_idx++) {
				uint64_t table_col_idx = stmt.columns.size() == 0 ? col_idx : named_column_map[col_idx];
				assert(table_col_idx < table->columns.size());
				binder.target_type = table->columns[table_col_idx].type;
				auto bound_expr = binder.Bind(expression_list[col_idx]);
				list.push_back(move(bound_expr));
			}
			result->values.push_back(move(list));
		}
	}
	return move(result);
}
