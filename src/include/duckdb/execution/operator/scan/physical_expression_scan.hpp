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

namespace duckdb {

//! The PhysicalExpressionScan scans a set of expressions
class PhysicalExpressionScan : public PhysicalOperator {
public:
	PhysicalExpressionScan(vector<LogicalType> types, vector<vector<unique_ptr<Expression>>> expressions,
	                       idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::EXPRESSION_SCAN, move(types), estimated_cardinality),
	      expressions(move(expressions)) {
	}

	//! The set of expressions to scan
	vector<vector<unique_ptr<Expression>>> expressions;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;

public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

} // namespace duckdb
