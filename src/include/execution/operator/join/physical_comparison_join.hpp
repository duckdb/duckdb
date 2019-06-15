//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/join/physical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/operator/join/physical_join.hpp"

namespace duckdb {

//! PhysicalJoin represents the base class of the join operators
class PhysicalComparisonJoin : public PhysicalJoin {
public:
	PhysicalComparisonJoin(LogicalOperator &op, PhysicalOperatorType type, vector<JoinCondition> cond,
	                       JoinType join_type);

	vector<JoinCondition> conditions;

public:
	string ExtraRenderInformation() const override;
};

} // namespace duckdb
