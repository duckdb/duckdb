#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/copy_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(CopyStatement &statement) {
	if (statement.select_statement) {
		// COPY from a query
		CreatePlan(*statement.select_statement);
		assert(root);
		auto copy = make_unique<LogicalCopy>(nullptr, move(statement.info));
		copy->AddChild(move(root));
		root = move(copy);
	} else {
		assert(!statement.info->table.empty());
		// COPY from a table
		auto table =
		    context.db.catalog.GetTable(context.ActiveTransaction(), statement.info->schema, statement.info->table);
		auto copy = make_unique<LogicalCopy>(table, move(statement.info));
		root = move(copy);
	}
}
