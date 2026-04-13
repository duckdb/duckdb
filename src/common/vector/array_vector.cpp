#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

VectorArrayBuffer::VectorArrayBuffer(unique_ptr<Vector> child_vector, idx_t array_size, idx_t initial_capacity)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::ARRAY_BUFFER), child(std::move(child_vector)),
      array_size(array_size), size(initial_capacity) {
	D_ASSERT(array_size != 0);
	validity.Resize(initial_capacity);
}

VectorArrayBuffer::VectorArrayBuffer(const LogicalType &array, idx_t initial)
    : VectorArrayBuffer(make_uniq<Vector>(ArrayType::GetChildType(array), initial * ArrayType::GetSize(array)),
                        ArrayType::GetSize(array), initial) {
}

VectorArrayBuffer::~VectorArrayBuffer() {
}

Vector &VectorArrayBuffer::GetChild() {
	return *child;
}

idx_t VectorArrayBuffer::GetArraySize() {
	return array_size;
}

idx_t VectorArrayBuffer::GetChildSize() {
	return size * array_size;
}

void VectorArrayBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
}

template <class T>
T &ArrayVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.buffer);
	D_ASSERT(vector.buffer->GetBufferType() == VectorBufferType::ARRAY_BUFFER);
	return vector.buffer->template Cast<VectorArrayBuffer>().GetChild();
}

const Vector &ArrayVector::GetEntry(const Vector &vector) {
	return GetEntryInternal<const Vector>(vector);
}

Vector &ArrayVector::GetEntry(Vector &vector) {
	return GetEntryInternal<Vector>(vector);
}

idx_t ArrayVector::GetTotalSize(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetTotalSize(child);
	}
	return vector.buffer->Cast<VectorArrayBuffer>().GetChildSize();
}

} // namespace duckdb
