#include "duckdb/common/types/vector_buffer.hpp"

#include "duckdb/common/assert.hpp"

using namespace duckdb;
using namespace std;

VectorBuffer::VectorBuffer(idx_t data_size) : type(VectorBufferType::STANDARD_BUFFER) {
	if (data_size > 0) {
		data = unique_ptr<data_t[]>(new data_t[data_size]);
	}
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(TypeId type, idx_t count) {
	return make_buffer<VectorBuffer>(count * GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateConstantVector(TypeId type) {
	return make_buffer<VectorBuffer>(GetTypeIdSize(type));
}

VectorStringBuffer::VectorStringBuffer() : VectorBuffer(VectorBufferType::STRING_BUFFER) {
}
