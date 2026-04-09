#include "duckdb/common/vector/sequence_vector.hpp"

namespace duckdb {

SequenceBuffer::SequenceBuffer(int64_t start_p, int64_t increment_p, int64_t count_p)
    : VectorBuffer(VectorType::SEQUENCE_VECTOR, VectorBufferType::SEQUENCE_BUFFER), start(start_p),
      increment(increment_p), count(count_p) {
}

idx_t SequenceBuffer::GetAllocationSize() const {
	idx_t size = VectorBuffer::GetAllocationSize();
	size += sizeof(int64_t) * 3;
	return size;
}

void SequenceBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	if (count == 0) {
		return;
	}
	D_ASSERT(vector_type == VectorType::SEQUENCE_VECTOR);
}

void SequenceVector::GetSequence(const Vector &vector, int64_t &start, int64_t &increment, int64_t &sequence_count) {
	D_ASSERT(vector.GetVectorType() == VectorType::SEQUENCE_VECTOR);
	auto &data = vector.buffer->Cast<SequenceBuffer>();
	start = data.start;
	increment = data.increment;
	sequence_count = data.count;
}

void SequenceVector::GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
	int64_t sequence_count;
	GetSequence(vector, start, increment, sequence_count);
}

} // namespace duckdb
