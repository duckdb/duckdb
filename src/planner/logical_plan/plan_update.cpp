#include "parser/expression/bound_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/statement/update_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(UpdateStatement &statement) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	AcceptChild(&statement.table);
	if (!root || root->type != LogicalOperatorType::GET) {
		throw Exception("Cannot create update node without table scan!");
	}
	auto get = (LogicalGet *)root.get();
	// create the filter (if any)
	if (statement.condition) {
		VisitExpression(&statement.condition);
		auto filter = make_unique<LogicalFilter>(move(statement.condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// scan the table for the referenced columns in the update clause
	auto &table = get->table;
	vector<column_t> column_ids;
	vector<unique_ptr<Expression>> projection_expressions;
	for (size_t i = 0; i < statement.columns.size(); i++) {
		auto &colname = statement.columns[i];

		if (!table->ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname.c_str());
		}
		auto &column = table->GetColumn(colname);
		column_ids.push_back(column.oid);
		// now resolve the expression
		if (statement.expressions[i]->type == ExpressionType::VALUE_DEFAULT) {
			// resolve the type of the DEFAULT expression
			statement.expressions[i]->return_type = column.type;
		} else {
			// visit this child and resolve its type
			VisitExpression(&statement.expressions[i]);
			statement.expressions[i]->ResolveType();
			// now check if we have to create a cast
			auto expression = move(statement.expressions[i]);
			if (expression->return_type != column.type) {
				// differing types, create a cast
				expression = make_unique<CastExpression>(column.type, move(expression));
			}
			statement.expressions[i] =
			    make_unique<BoundExpression>(expression->return_type, projection_expressions.size());
			projection_expressions.push_back(move(expression));
		}
	}
	if (projection_expressions.size() > 0) {
		// have to create a projection first
		// add the row id column to the projection list
		projection_expressions.push_back(make_unique<BoundExpression>(TypeId::POINTER, get->column_ids.size() - 1));
		// now create the projection
		auto proj = make_unique<LogicalProjection>(move(projection_expressions));
		proj->AddChild(move(root));
		root = move(proj);
	}
	// create the update node
	auto update = make_unique<LogicalUpdate>(table, column_ids, move(statement.expressions));
	update->AddChild(move(root));
	root = move(update);
}
