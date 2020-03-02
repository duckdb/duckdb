#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/statement/bound_delete_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundDeleteStatement &stmt) {
	// we require row ids for the deletion
	require_row_id = true;
	// create the table scan
	auto root = CreatePlan(*stmt.table);
	assert(root->type == LogicalOperatorType::GET);

	auto &get = (LogicalGet &)*root;
	// create the filter (if any)
	if (stmt.condition) {
		PlanSubqueries(&stmt.condition, &root);
		auto filter = make_unique<LogicalFilter>(move(stmt.condition));
		filter->AddChild(move(root));
		root = move(filter);
	}
	// create the delete node
	auto del = make_unique<LogicalDelete>(get.table);
	del->AddChild(move(root));
	// we push an BoundColumnRef binding to the row_id index
	del->expressions.push_back(make_unique<BoundColumnRefExpression>(
	    TypeId::INT64, ColumnBinding(get.table_index, get.column_ids.size() - 1)));
	return move(del);
}
