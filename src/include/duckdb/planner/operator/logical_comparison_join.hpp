//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	explicit LogicalComparisonJoin(JoinType type,
	                               LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! Used for duplicate-eliminated joins
	vector<LogicalType> delim_types;

public:
	string ParamsToString() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);
	static void Deserialize(LogicalComparisonJoin &comparison_join, LogicalDeserializationState &state,
	                        FieldReader &reader);

public:
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              unique_ptr<Expression> condition);
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              vector<JoinCondition> conditions,
	                                              vector<unique_ptr<Expression>> arbitrary_expressions);

	static void ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
	                                  unique_ptr<LogicalOperator> &right_child, unique_ptr<Expression> condition,
	                                  vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);
	static void ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
	                                  unique_ptr<LogicalOperator> &right_child,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);
	static void ExtractJoinConditions(JoinType type, unique_ptr<LogicalOperator> &left_child,
	                                  unique_ptr<LogicalOperator> &right_child,
	                                  const unordered_set<idx_t> &left_bindings,
	                                  const unordered_set<idx_t> &right_bindings,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);
};

} // namespace duckdb
