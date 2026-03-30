#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

template <class T>
T &ArrayVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::ARRAY_BUFFER);
	return vector.auxiliary->template Cast<VectorArrayBuffer>().GetChild();
}

const Vector &ArrayVector::GetEntry(const Vector &vector) {
	return GetEntryInternal<const Vector>(vector);
}

Vector &ArrayVector::GetEntry(Vector &vector) {
	return GetEntryInternal<Vector>(vector);
}

idx_t ArrayVector::GetTotalSize(const Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::ARRAY);
	D_ASSERT(vector.auxiliary);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ArrayVector::GetTotalSize(child);
	}
	return vector.auxiliary->Cast<VectorArrayBuffer>().GetChildSize();
}

} // namespace duckdb
