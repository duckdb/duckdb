#include "parser/statement/select_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(SelectStatement &statement) {
	auto expected_column_count = statement.node->GetSelectCount();
	CreatePlan(*statement.node);
	// prune the root node
	assert(root);
	auto prune = make_unique<LogicalPruneColumns>(expected_column_count);
	prune->AddChild(move(root));
	root = move(prune);
}
