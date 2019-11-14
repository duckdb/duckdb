#include "duckdb/planner/operator/logical_any_join.hpp"

using namespace duckdb;
using namespace std;

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::ANY_JOIN) {
}

string LogicalAnyJoin::ParamsToString() const {
	return "[" + condition->ToString() + "]";
}
