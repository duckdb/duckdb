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
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment, int64_t &sequence_count);
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment);
};

} // namespace duckdb
