#include "parser/statement/select_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(SelectStatement &statement) {
	CreatePlan(*statement.node);
}
