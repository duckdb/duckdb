//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_try.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalTry wraps the plan of an uncorrelated scalar subquery that appears below a TRY expression.
//! It is an error boundary: execution errors raised while computing the child plan are converted into a single
//! NULL row. The child plan is guaranteed to produce exactly one row, so the boundary also produces exactly one row.
class LogicalTry : public LogicalOperator {
	LogicalTry();

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TRY;

public:
	explicit LogicalTry(unique_ptr<LogicalOperator> child);

	vector<ColumnBinding> GetColumnBindings() override;
	idx_t EstimateCardinality(ClientContext &context) override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
