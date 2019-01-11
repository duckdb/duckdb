#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/copy_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(CopyStatement &statement) {
	if (!statement.table.empty()) {
		auto table = context.db.catalog.GetTable(context.ActiveTransaction(), statement.schema, statement.table);
		auto copy = make_unique<LogicalCopy>(table, move(statement.file_path), move(statement.is_from),
		                                     move(statement.delimiter), move(statement.quote), move(statement.escape),
		                                     move(statement.select_list));
		root = move(copy);
	} else {
		auto copy = make_unique<LogicalCopy>(move(statement.file_path), move(statement.is_from),
		                                     move(statement.delimiter), move(statement.quote), move(statement.escape));
		CreatePlan(*statement.select_statement);
		assert(root);
		copy->AddChild(move(root));
		root = move(copy);
	}
}
