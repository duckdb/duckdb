#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/statement/insert_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(InsertStatement &statement) {
	auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.schema, statement.table);
	auto insert = make_unique<LogicalInsert>(table);

	if (statement.columns.size() > 0) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		map<string, int> column_name_map;
		for (size_t i = 0; i < statement.columns.size(); i++) {
			column_name_map[statement.columns[i]] = i;
		}
		for (size_t i = 0; i < table->columns.size(); i++) {
			auto &col = table->columns[i];
			auto entry = column_name_map.find(col.name);
			if (entry == column_name_map.end()) {
				// column not specified, set index to -1
				insert->column_index_map.push_back(-1);
			} else {
				// column was specified, set to the index
				insert->column_index_map.push_back(entry->second);
			}
		}
	}

	if (statement.select_statement) {
		// insert from select statement
		// parse select statement and add to logical plan
		CreatePlan(*statement.select_statement);
		assert(root);
		insert->AddChild(move(root));
		root = move(insert);
	} else {
		if (statement.columns.size() == 0) {
			if (statement.values[0].size() != table->columns.size()) {
				throw SyntaxException("table %s has %d columns but %d values were supplied", table->name.c_str(),
				                      table->columns.size(), statement.values[0].size());
			}
		} else {
			if (statement.values[0].size() != statement.columns.size()) {
				throw SyntaxException("Column name/value mismatch: %d values for %d columns",
				                      statement.values[0].size(), statement.columns.size());
			}
		}

		//  visit the expressions
		for (auto &expression_list : statement.values) {
			for (size_t col_idx = 0; col_idx < expression_list.size(); col_idx++) {
				auto &expression = expression_list[col_idx];
				if (expression->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
					size_t table_col_idx = col_idx;
					if (statement.columns.size() > 0) { //  named cols
						table_col_idx = insert->column_index_map[col_idx];
					}
					if (table_col_idx > table->columns.size() - 1) {
						throw Exception("Too many values for insert");
					}
					auto cast = make_unique_base<Expression, CastExpression>(table->columns[table_col_idx].type,
					                                                         move(expression));
					expression_list[col_idx] = move(cast);
				}

				VisitExpression(&expression_list[col_idx]);
			}
		}
		// insert from constants
		// check if the correct amount of constants are supplied

		insert->insert_values = move(statement.values);
		root = move(insert);
	}
}
