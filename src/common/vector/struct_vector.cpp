#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

vector<Vector> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::UNION ||
	         vector.GetType().id() == LogicalTypeId::VARIANT ||
	         vector.GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return StructVector::GetEntries(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.auxiliary->Cast<VectorStructBuffer>().GetChildren();
}

const vector<Vector> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

} // namespace duckdb
