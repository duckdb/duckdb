//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalAggregate represents an aggregate operation with (optional) GROUP BY
//! operator.
class LogicalWindow : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_WINDOW;

public:
	explicit LogicalWindow(idx_t window_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_WINDOW), window_index(window_index) {
	}

	idx_t window_index;

public:
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);
	vector<idx_t> GetTableIndex() const override;
	string GetName() const override;

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
