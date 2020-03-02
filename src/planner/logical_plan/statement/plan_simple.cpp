#include "duckdb/planner/logical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/statement/bound_simple_statement.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> LogicalPlanGenerator::CreatePlan(BoundSimpleStatement &stmt) {
	LogicalOperatorType type;
	switch (stmt.type) {
	case StatementType::ALTER:
		type = LogicalOperatorType::ALTER;
		break;
	case StatementType::DROP:
		type = LogicalOperatorType::DROP;
		break;
	case StatementType::PRAGMA:
		type = LogicalOperatorType::PRAGMA;
		break;
	case StatementType::TRANSACTION:
		type = LogicalOperatorType::TRANSACTION;
		break;
	default:
		throw NotImplementedException("Unimplemented type for LogicalSimple!");
	}
	return make_unique<LogicalSimple>(type, move(stmt.info));
}
