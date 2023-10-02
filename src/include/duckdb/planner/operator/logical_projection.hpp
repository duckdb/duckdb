//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalProjection represents the projection list in a SELECT clause
class LogicalProjection : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_PROJECTION;

public:
	LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list);

	idx_t table_index;

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
