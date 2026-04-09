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

class SequenceBuffer : public VectorBuffer {
public:
	explicit SequenceBuffer(int64_t start, int64_t increment, int64_t count);

	int64_t start;
	int64_t increment;
	//! FIXME: should not be necessary once vector has count
	int64_t count;

public:
	idx_t GetAllocationSize() const override;
	Value GetValue(const LogicalType &type, idx_t index) const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) override;
};

struct SequenceVector {
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment, int64_t &sequence_count);
	static void GetSequence(const Vector &vector, int64_t &start, int64_t &increment);
};

} // namespace duckdb
