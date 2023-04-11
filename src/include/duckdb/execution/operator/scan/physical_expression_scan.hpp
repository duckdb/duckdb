//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_expression_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! The PhysicalExpressionScan scans a set of expressions
class PhysicalExpressionScan : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXPRESSION_SCAN;

public:
	PhysicalExpressionScan(vector<LogicalType> types, vector<vector<unique_ptr<Expression>>> expressions,
	                       idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::EXPRESSION_SCAN, std::move(types), estimated_cardinality),
	      expressions(std::move(expressions)) {
	}

	//! The set of expressions to scan
	vector<vector<unique_ptr<Expression>>> expressions;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}

public:
	bool IsFoldable() const;
	void EvaluateExpression(ClientContext &context, idx_t expression_idx, DataChunk *child_chunk,
	                        DataChunk &result) const;
};

} // namespace duckdb
