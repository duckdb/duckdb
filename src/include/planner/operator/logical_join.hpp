//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include <unordered_set>

namespace duckdb {

struct JoinCondition {
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
	ExpressionType comparison;
};

enum JoinSide { NONE, LEFT, RIGHT, BOTH };

//! LogicalJoin represents a join between two relations
class LogicalJoin : public LogicalOperator {
public:
	LogicalJoin(JoinType type) :
		LogicalOperator(LogicalOperatorType::JOIN), type(type),
		is_duplicate_eliminated(false), null_values_are_equal(false) {
	}

	vector<string> GetNames() override;

	// Gets the set of table references that are reachable from this node
	static void GetTableReferences(LogicalOperator &op, std::unordered_set<size_t> &bindings);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! The type of the join (INNER, OUTER, etc...)
	JoinType type;
	//! Whether or not the join is a special "duplicate eliminated" join. This join type is only used for subquery flattening, and involves performing duplicate elimination on the LEFT side which is then pushed into the RIGHT side. 
	bool is_duplicate_eliminated;
	//! The set of columns that will be duplicate eliminated from the LHS and pushed into the RHS
	vector<unique_ptr<Expression>> duplicate_eliminated_columns;
	//! Whether or not the join should consider NULL values to be identical to one another (i.e. NULL values CAN have join partners)
	bool null_values_are_equal;

	string ParamsToString() const override;

	size_t ExpressionCount() override;
	Expression *GetExpression(size_t index) override;
	void ReplaceExpression(std::function<unique_ptr<Expression>(unique_ptr<Expression> expression)> callback,
	                       size_t index) override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
