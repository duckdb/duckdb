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

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vector_type, const LogicalType &type, const idx_t size) {
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

void VectorListBuffer::Reserve(const Vector &to_append, idx_t to_reserve) {
	if (to_reserve > capacity) {
		idx_t new_capacity = (to_reserve) / STANDARD_VECTOR_SIZE + ((to_reserve) % STANDARD_VECTOR_SIZE != 0);
		new_capacity *= STANDARD_VECTOR_SIZE;
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(to_append, size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(to_append, size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(Value &insert) {
	if (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
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
