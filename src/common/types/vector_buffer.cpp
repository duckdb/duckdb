#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/assert.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(VectorType vector_type, const LogicalType &type) {
	return make_buffer<VectorBuffer>(vector_type, type, GetTypeIdSize(type.InternalType()));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vector_type, const LogicalType &type) {
	return make_buffer<VectorBuffer>(vector_type, type, STANDARD_VECTOR_SIZE * GetTypeIdSize(type.InternalType()));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vector_type, PhysicalType type) {
	return make_buffer<VectorBuffer>(vector_type, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer() : VectorBuffer(VectorBufferType::LIST_BUFFER) {
}

void VectorListBuffer::SetChild(unique_ptr<Vector> new_child) {
	child = move(new_child);
	capacity = STANDARD_VECTOR_SIZE;
}

void VectorListBuffer::Append(Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	while (size + to_append_size - source_offset > capacity) {
		// Drink chocomel to grow strong
		child->Resize(capacity);
		capacity *= 2;
	}
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);

	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(Value &insert) {
	if (size + 1 > capacity) {
		// Drink chocomel to grow strong
		child->Resize(capacity);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

VectorListBuffer::~VectorListBuffer() {
}

ManagedVectorBuffer::ManagedVectorBuffer(unique_ptr<BufferHandle> handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}

} // namespace duckdb
