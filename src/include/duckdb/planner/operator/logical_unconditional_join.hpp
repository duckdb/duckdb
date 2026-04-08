//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_unconditional_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {

//! LogicalUnconditionalJoin represents a join between two relations
//! where the join condition is implicit (cross product, position, etc.)
class LogicalUnconditionalJoin : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	explicit LogicalUnconditionalJoin(LogicalOperatorType logical_type) : LogicalOperator(logical_type) {};

public:
	LogicalUnconditionalJoin(LogicalOperatorType logical_type, unique_ptr<LogicalOperator> left,
	                         unique_ptr<LogicalOperator> right);

public:
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
