#include "planner/logical_plan_generator.hpp"
#include "planner/operator/logical_insert.hpp"
#include "planner/statement/bound_insert_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundInsertStatement &stmt) {
	auto table = stmt.table;

	auto insert = make_unique<LogicalInsert>(table);
	insert->column_index_map = stmt.column_index_map;
	if (stmt.select_statement) {
		// insert from select statement
		// parse select statement and add to logical plan
		auto root = CreatePlan(*stmt.select_statement);
		insert->AddChild(move(root));
	} else {
		//  visit the expressions
		for (auto &expression_list : stmt.values) {
			for (size_t col_idx = 0; col_idx < expression_list.size(); col_idx++) {
//				auto &expression = expression_list[col_idx];
				throw Exception("FIXME: Plan insert statement values");
				// if (expression->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
				// 	size_t table_col_idx = col_idx;
				// 	if (stmt.columns.size() > 0) { //  named cols
				// 		table_col_idx = insert->column_index_map[col_idx];
				// 	}
				// 	if (table_col_idx > table->columns.size() - 1) {
				// 		throw Exception("Too many values for insert");
				// 	}
				// 	auto cast = make_unique_base<Expression, CastExpression>(table->columns[table_col_idx].type,
				// 	                                                         move(expression));
				// 	expression_list[col_idx] = move(cast);
				// }

				// VisitExpression(&expression_list[col_idx]);
				// expression_list[col_idx]->ResolveType();
			}
		}
		insert->insert_values = move(stmt.values);
	}
	return move(insert);
}
