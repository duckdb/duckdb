//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <unordered_set>

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;
class Deserializer;
class Serializer;
enum class JoinRefType : uint8_t;
struct TableIndex;

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	explicit LogicalComparisonJoin(JoinType type,
	                               LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! Used for duplicate-eliminated MARK joins
	vector<LogicalType> mark_types;
	//! The set of columns that will be duplicate eliminated from the LHS and pushed into the RHS
	vector<unique_ptr<Expression>> duplicate_eliminated_columns;
	//! If this is a DelimJoin, whether it has been flipped to de-duplicating the RHS instead
	bool delim_flipped = false;
	//! (If join_type == MARK) can this comparison join be converted from a mark join to semi
	bool convert_mark_to_semi = true;
	//! Scans where we should push generated filters into (if any)
	unique_ptr<JoinFilterPushdownInfo> filter_pushdown;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

public:
	static unique_ptr<LogicalOperator> CreateJoin(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                              unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              unique_ptr<Expression> condition);
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, JoinRefType ref_type,
	                                              unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              vector<JoinCondition> conditions);

	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  const unordered_set<TableIndex> &left_bindings,
	                                  const unordered_set<TableIndex> &right_bindings,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions);
	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions);
	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions);

	bool HasEquality(idx_t &range_count) const;
	bool HasArbitraryConditions() const;
};

} // namespace duckdb
