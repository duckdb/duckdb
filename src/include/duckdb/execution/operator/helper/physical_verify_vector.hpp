//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_verify_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! The PhysicalVerifyVector operator is a streaming operator that emits the same data as it ingests - but in a
//! different format
// There are different configurations
// * Dictionary: Transform every vector into a dictionary vector where the underlying vector has gaps and is reversed
//       i.e. original: FLAT [1, 2, 3]
//            modified: BASE: [NULL, 3, NULL, 2, NULL, 1]   OFFSETS: [5, 3, 1]
// * Constant: Decompose every DataChunk into single-row constant vectors
//       i.e. original: FLAT [1, 2, 3]
//            modified: chunk #1 - CONSTANT [1]
//                      chunk #2 - CONSTANT [2]
//                      chunk #3 - CONSTANT [3]
// * Sequence & Constant: Decompose every DataChunk into constant or sequence vectors based on the longest possibility
//            original:  a: [1, 1, 20, 15, 13]   b: [1, 10, 100, 101, 102]
//            modified:  chunk #1 - a: CONSTANT [1, 1]          b: DICTIONARY [1, 10]
//                       chunk #2 - a: DICTIONARY [20, 15, 13]  b: SEQUENCE [100, 101, 102]
// * Nested Shuffle: Reshuffle list vectors so that offsets are not contiguous
//            original: [[1, 2], [3, 4]] - BASE: [1, 2, 3, 4] LISTS: [offset: 0, length: 2][offset: 2, length: 2]
//            modified: [[1, 2], [3, 4]] - BASE: [3, 4, 1, 2] LISTS: [offset: 2, length: 2][offset: 0, length: 2]
class PhysicalVerifyVector : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::VERIFY_VECTOR;

public:
	explicit PhysicalVerifyVector(unique_ptr<PhysicalOperator> child);

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}
};

} // namespace duckdb
