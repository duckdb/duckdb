#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/planner/operator/logical_show.hpp"

using namespace duckdb;
using namespace std;

BoundStatement Binder::Bind(ShowStatement &stmt) {

	BoundStatement result;

  auto plan = Bind(*stmt.info->query);
  stmt.info->types = plan.types;
  stmt.info->aliases = plan.names;
	
	auto logical_plan_unopt = plan.plan->ToString();
	auto show = make_unique<LogicalShow>(move(plan.plan));
	show->types_select = plan.types;
  show->aliases = plan.names;

	result.plan = move(show);

  result.names = {"Field", "Type", "Not Null", "Default", "Key"};
	result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::BOOLEAN};
	return result;
}
