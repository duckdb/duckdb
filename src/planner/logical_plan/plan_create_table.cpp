#include "main/client_context.hpp"
#include "main/database.hpp"
#include "parser/statement/create_table_statement.hpp"
#include "planner/logical_plan_generator.hpp"
#include "planner/operator/list.hpp"
#include "parser/column_definition.hpp"

using namespace duckdb;
using namespace std;

void LogicalPlanGenerator::CreatePlan(CreateTableStatement &statement) {
	if (statement.query) {
		CreatePlan(*statement.query);
		root->ResolveOperatorTypes();
		auto names = root->GetNames();
		auto &types = root->types;
		assert(names.size() == types.size());
		for(size_t i = 0; i < names.size(); i++) {
			statement.info->columns.push_back(ColumnDefinition(names[i], types[i]));
		}
	}
	// bind the schema
	auto schema = context.db.catalog.GetSchema(context.ActiveTransaction(), statement.info->schema);
	// create the logical operator
	auto create_table = make_unique<LogicalCreateTable>(schema, move(statement.info));
	if (root) {
		create_table->children.push_back(move(root));
	}
	root = move(create_table);
}
