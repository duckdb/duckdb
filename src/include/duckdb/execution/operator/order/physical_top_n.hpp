//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/physical_top_n.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Represents a physical ordering of the data. Note that this will not change
//! the data but only add a selection vector.
class PhysicalTopN : public PhysicalOperator {
public:
	PhysicalTopN(LogicalOperator &op, vector<BoundOrderByNode> orders, index_t limit, index_t offset)
	    : PhysicalOperator(PhysicalOperatorType::TOP_N, op.types), orders(move(orders)), limit(limit), offset(offset),
	      heap_size(0) {
	}

	vector<BoundOrderByNode> orders;
	index_t limit;
	index_t offset;
	index_t heap_size;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void CalculateHeapSize(index_t rows);
};

} // namespace duckdb
