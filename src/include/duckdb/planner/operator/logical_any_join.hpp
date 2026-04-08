//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_any_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class Deserializer;
class Serializer;
enum class JoinType : uint8_t;

//! LogicalAnyJoin represents a join with an arbitrary expression as JoinCondition
class LogicalAnyJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_ANY_JOIN;

public:
	explicit LogicalAnyJoin(JoinType type);

	//! The JoinCondition on which this join is performed
	unique_ptr<Expression> condition;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
