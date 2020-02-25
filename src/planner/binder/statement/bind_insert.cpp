#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/planner/statement/bound_insert_statement.hpp"

using namespace std;

namespace duckdb {

static void CheckInsertColumnCountMismatch(int64_t expected_columns, int64_t result_columns, bool columns_provided,
                                           const char *tname) {
	if (result_columns != expected_columns) {
		string msg = StringUtil::Format(!columns_provided ? "table %s has %lld columns but %lld values were supplied"
		                                                  : "Column name/value mismatch for insert on %s: "
		                                                    "expected %lld columns but %lld values were supplied",
		                                tname, expected_columns, result_columns);
		throw BinderException(msg);
	}
}

unique_ptr<BoundSQLStatement> Binder::Bind(InsertStatement &stmt) {
	auto result = make_unique<BoundInsertStatement>();
	auto table = context.catalog.GetTable(context, stmt.schema, stmt.table);
	assert(table);

	result->table = table;

	vector<idx_t> named_column_map;
	if (stmt.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		unordered_map<string, idx_t> column_name_map;
		for (idx_t i = 0; i < stmt.columns.size(); i++) {
			column_name_map[stmt.columns[i]] = i;
			auto entry = table->name_map.find(stmt.columns[i]);
			if (entry == table->name_map.end()) {
				throw BinderException("Column %s not found in table %s", stmt.columns[i].c_str(), table->name.c_str());
			}
			if (entry->second == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			result->expected_types.push_back(table->columns[entry->second].type);
			named_column_map.push_back(entry->second);
		}
		for (idx_t i = 0; i < result->table->columns.size(); i++) {
			auto &col = result->table->columns[i];
			auto entry = column_name_map.find(col.name);
			if (entry == column_name_map.end()) {
				// column not specified, set index to INVALID_INDEX
				result->column_index_map.push_back(INVALID_INDEX);
			} else {
				// column was specified, set to the index
				result->column_index_map.push_back(entry->second);
			}
		}
	} else {
		for (idx_t i = 0; i < result->table->columns.size(); i++) {
			result->expected_types.push_back(table->columns[i].type);
		}
	}

	// bind the default values
	BindDefaultValues(table->columns, result->bound_defaults);
	if (!stmt.select_statement) {
		return move(result);
	}

	idx_t expected_columns = stmt.columns.size() == 0 ? result->table->columns.size() : stmt.columns.size();
	// special case: check if we are inserting from a VALUES statement
	if (stmt.select_statement->node->type == QueryNodeType::SELECT_NODE) {
		auto &node = (SelectNode &)*stmt.select_statement->node;
		if (node.from_table->type == TableReferenceType::EXPRESSION_LIST) {
			auto &expr_list = (ExpressionListRef &)*node.from_table;
			assert(expr_list.values.size() > 0);
			CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), stmt.columns.size() != 0,
			                               result->table->name.c_str());

			// VALUES list!
			for (idx_t col_idx = 0; col_idx < expected_columns; col_idx++) {
				idx_t table_col_idx = stmt.columns.size() == 0 ? col_idx : named_column_map[col_idx];
				assert(table_col_idx < table->columns.size());

				// set the expected types as the types for the INSERT statement
				auto &column = table->columns[table_col_idx];
				expr_list.expected_types.push_back(column.type);
				expr_list.expected_names.push_back(column.name);

				// now replace any DEFAULT values with the corresponding default expression
				for (idx_t list_idx = 0; list_idx < expr_list.values.size(); list_idx++) {
					if (expr_list.values[list_idx][col_idx]->type == ExpressionType::VALUE_DEFAULT) {
						// DEFAULT value! replace the entry
						if (column.default_value) {
							expr_list.values[list_idx][col_idx] = column.default_value->Copy();
						} else {
							expr_list.values[list_idx][col_idx] =
							    make_unique<ConstantExpression>(column.type, Value(GetInternalType(column.type)));
						}
					}
				}
			}
		}
	}

	// bind the select statement, if any
	result->select_statement = unique_ptr_cast<BoundSQLStatement, BoundSelectStatement>(Bind(*stmt.select_statement));
	CheckInsertColumnCountMismatch(expected_columns, result->select_statement->node->types.size(),
	                               stmt.columns.size() != 0, result->table->name.c_str());

	return move(result);
}

} // namespace duckdb
