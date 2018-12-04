//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/operator/join/physical_join.hpp
//
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/physical_operator.hpp"

#include "planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalJoin represents the base class of the join operators
class PhysicalJoin : public PhysicalOperator {
  public:
	PhysicalJoin(PhysicalOperatorType type, std::vector<JoinCondition> cond,
	             JoinType join_type);

	std::vector<std::string> GetNames() override;
	std::vector<TypeId> GetTypes() override;

	std::string ExtraRenderInformation() override;

	std::vector<JoinCondition> conditions;
	JoinType type;
};
} // namespace duckdb
