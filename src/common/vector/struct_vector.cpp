#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"

namespace duckdb {

VectorStructBuffer::VectorStructBuffer() : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER) {
}

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, idx_t capacity)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		children.emplace_back(child_type.second, capacity);
	}
	validity.Resize(capacity);
}

VectorStructBuffer::VectorStructBuffer(Vector &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER) {
	auto &other_vector = StructVector::GetEntries(other);
	for (auto &child_vector : other_vector) {
		children.emplace_back(child_vector, sel, count);
	}
	// slice the validity mask of the original struct
	auto &original_validity = other.GetBuffer()->GetValidityMask();
	if (count > STANDARD_VECTOR_SIZE) {
		validity.Resize(count);
	}
	validity.CopySel(original_validity, sel, 0, 0, count);
}

void VectorStructBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
	for (auto &child : children) {
		child.SetVectorType(new_vector_type);
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

vector<Vector> &StructVector::GetEntries(Vector &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::STRUCT || vector.GetType().id() == LogicalTypeId::UNION ||
	         vector.GetType().id() == LogicalTypeId::VARIANT ||
	         vector.GetType().id() == LogicalTypeId::AGGREGATE_STATE);

	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		throw InternalException("Struct vectors cannot be dictionary vectors");
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.buffer);
	D_ASSERT(vector.buffer->GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.buffer->Cast<VectorStructBuffer>().GetChildren();
}

const vector<Vector> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

} // namespace duckdb
