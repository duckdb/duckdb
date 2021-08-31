//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_cross_product.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
//! PhysicalCrossProduct represents a cross product between two tables
class PhysicalCrossProduct : public PhysicalOperator {
public:
	PhysicalCrossProduct(vector<LogicalType> types, unique_ptr<PhysicalOperator> left,
	                     unique_ptr<PhysicalOperator> right, idx_t estimated_cardinality);

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
