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

	string ExtraRenderInformation() const override;

	vector<JoinCondition> conditions;
};

} // namespace duckdb
