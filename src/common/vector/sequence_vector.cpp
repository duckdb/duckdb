#include "duckdb/common/vector/sequence_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

SequenceBuffer::SequenceBuffer(int64_t start_p, int64_t increment_p, count_t count_p)
    : VectorBuffer(VectorType::SEQUENCE_VECTOR, VectorBufferType::SEQUENCE_BUFFER, count_p), start(start_p),
      increment(increment_p) {
}

idx_t SequenceBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	return GetTypeIdSize(type.InternalType()) * count;
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

string SequenceBuffer::ToString(const LogicalType &type, idx_t count) const {
	string retval;
	for (idx_t i = 0; i < count; i++) {
		retval += to_string(start + static_cast<int64_t>(static_cast<uint64_t>(increment) * i));
		if (i < count - 1) {
			retval += ", ";
		}
	}
	return retval;
}

Value SequenceBuffer::GetValue(const LogicalType &type, idx_t index) const {
	return Value::Numeric(type, start + static_cast<int64_t>(static_cast<uint64_t>(increment) * index));
}

buffer_ptr<VectorBuffer> SequenceBuffer::FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                              idx_t count) const {
	Vector flattened_vector(type, count);
	VectorOperations::GenerateSequence(flattened_vector, count, sel, start, increment);
	auto result = flattened_vector.GetBufferRef();
	result->SetVectorSize(count);
	return result;
}

void SequenceVector::GetSequence(const Vector &vector, int64_t &start, int64_t &increment) {
	D_ASSERT(vector.GetVectorType() == VectorType::SEQUENCE_VECTOR);
	auto &data = vector.Buffer().Cast<SequenceBuffer>();
	start = data.start;
	increment = data.increment;
}

} // namespace duckdb
