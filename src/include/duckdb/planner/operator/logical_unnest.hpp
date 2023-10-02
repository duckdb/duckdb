//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_unnest.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalUnnest represents the logical UNNEST operator.
class LogicalUnnest : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_UNNEST;

public:
	explicit LogicalUnnest(idx_t unnest_index)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_UNNEST), unnest_index(unnest_index) {
	}

	idx_t unnest_index;

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
