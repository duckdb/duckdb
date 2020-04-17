#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

#include "duckdb/common/assert.hpp"

using namespace duckdb;
using namespace std;

VectorBuffer::VectorBuffer(idx_t data_size) : type(VectorBufferType::STANDARD_BUFFER) {
	if (data_size > 0) {
		data = unique_ptr<data_t[]>(new data_t[data_size]);
	}
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(TypeId type) {
	return make_buffer<VectorBuffer>(STANDARD_VECTOR_SIZE * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(TypeId type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
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
