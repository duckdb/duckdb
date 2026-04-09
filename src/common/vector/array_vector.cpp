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

idx_t VectorArrayBuffer::GetArraySize() const {
	return array_size;
}

idx_t VectorArrayBuffer::GetChildSize() const {
	return size * array_size;
}

void VectorArrayBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
}

idx_t VectorArrayBuffer::GetAllocationSize() const {
	idx_t size = VectorBuffer::GetAllocationSize();
	size += validity.GetAllocationSize();
	size += child->GetAllocationSize();
	return size;
}

void VectorArrayBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	if (count == 0) {
		return;
	}
	D_ASSERT(type.InternalType() == PhysicalType::ARRAY);
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		count = 1;
	}
	SelectionVector child_sel(count * array_size);
	for (idx_t i = 0; i < count; i++) {
		auto offset = vector_type == VectorType::CONSTANT_VECTOR ? 0 : sel.get_index(i);
		for (idx_t r = 0; r < array_size; r++) {
			child_sel.set_index(i * array_size + r, offset * array_size + r);
		}
	}
	child->Verify(child_sel, count * array_size);
	// FIXME: verify validity, arrays have the same validity rules as children
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
