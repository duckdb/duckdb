#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/create_view_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(CreateViewStatement &statement) {
	// bind the schema
	auto schema = context.db.catalog.GetSchema(context.ActiveTransaction(), statement.info->schema);
	// create the logical operator
	root = make_unique<LogicalCreateView>(schema, move(statement.info));
}
