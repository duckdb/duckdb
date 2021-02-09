#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

#include "duckdb/common/assert.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(PhysicalType type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(VectorType vectorType, const LogicalType &logicalType,
                                                            PhysicalType type) {
	return make_buffer<VectorBuffer>(vectorType, logicalType, GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vectorType, const LogicalType &logicalType,
                                                            PhysicalType type) {
	return make_buffer<VectorBuffer>(vectorType, logicalType, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(VectorType vectorType, PhysicalType type) {
	return make_buffer<VectorBuffer>(vectorType, STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::~VectorStructBuffer() {
}

VectorListBuffer::VectorListBuffer() : VectorBuffer(VectorBufferType::LIST_BUFFER) {
}

void VectorListBuffer::SetChild(unique_ptr<ChunkCollection> new_child) {
	child = move(new_child);
}

VectorListBuffer::~VectorListBuffer() {
}

ManagedVectorBuffer::ManagedVectorBuffer(unique_ptr<BufferHandle> handle)
    : VectorBuffer(VectorBufferType::MANAGED_BUFFER), handle(move(handle)) {
}

ManagedVectorBuffer::~ManagedVectorBuffer() {
}

} // namespace duckdb
