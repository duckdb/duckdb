//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalCrossProduct represents a cross product between two relations
class LogicalCrossProduct : public LogicalOperator {
	LogicalCrossProduct() : LogicalOperator(LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {};

public:
	LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

public:
	static unique_ptr<LogicalOperator> Create(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right);

	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override;
};
} // namespace duckdb
