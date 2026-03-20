//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/sequence_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment, int64_t &sequence_count) {
		D_ASSERT(vector.GetVectorType() == VectorType::SEQUENCE_VECTOR);
		auto data = reinterpret_cast<int64_t *>(vector.buffer->GetData());
		start = data[0];
		increment = data[1];
		sequence_count = data[2];
	}
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
		int64_t sequence_count;
		GetSequence(vector, start, increment, sequence_count);
	}
};

} // namespace duckdb
