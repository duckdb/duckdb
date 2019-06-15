//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/scan/physical_expression_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/chunk_collection.hpp"
#include "execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalExpressionScan scans a set of expressions
class PhysicalExpressionScan : public PhysicalOperator {
public:
	PhysicalExpressionScan(vector<TypeId> types, vector<vector<unique_ptr<Expression>>> expressions)
	    : PhysicalOperator(PhysicalOperatorType::EXPRESSION_SCAN, types), expressions(move(expressions)) {
	}

	//! The set of expressions to scan
	vector<vector<unique_ptr<Expression>>> expressions;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalExpressionScanState : public PhysicalOperatorState {
public:
	PhysicalExpressionScanState(PhysicalOperator *child) : PhysicalOperatorState(child), expression_index(0) {
	}

	//! The current position in the scan
	index_t expression_index;
};
} // namespace duckdb
