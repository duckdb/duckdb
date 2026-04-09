#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

ShreddedVectorBuffer::ShreddedVectorBuffer(Vector &shredded_data_p)
    : VectorBuffer(VectorType::SHREDDED_VECTOR, VectorBufferType::SHREDDED_BUFFER),
      shredded_data(make_uniq<Vector>(Vector::Ref(shredded_data_p))) {
}

ShreddedVectorBuffer::~ShreddedVectorBuffer() {
}

buffer_ptr<VectorBuffer> ShreddedVectorBuffer::Flatten(const LogicalType &type, const SelectionVector &sel,
                                                       idx_t count) {
	Vector *source = shredded_data.get();
	// if a selection vector is provided, slice the shredded data first
	unique_ptr<Vector> sliced;
	if (sel.IsSet()) {
		sliced = make_uniq<Vector>(*shredded_data, sel, count);
		source = sliced.get();
	}
	// unshred the (optionally sliced) vector
	Vector unshredded_vector(LogicalType::VARIANT(), MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
	VariantUtils::UnshredVariantData(*source, unshredded_vector, count);
	// now flatten the unshredded vector
	unshredded_vector.Flatten(count);
	return unshredded_vector.GetBuffer();
}

const Vector &ShreddedVector::GetUnshreddedVector(const Vector &vec) {
	VerifyShreddedVector(vec);
	return StructVector::GetEntries(vec.buffer->Cast<ShreddedVectorBuffer>().GetChild())[0];
}

Vector &ShreddedVector::GetUnshreddedVector(Vector &vec) {
	VerifyShreddedVector(vec);
	return StructVector::GetEntries(vec.buffer->Cast<ShreddedVectorBuffer>().GetChild())[0];
}

const Vector &ShreddedVector::GetShreddedVector(const Vector &vec) {
	VerifyShreddedVector(vec);
	return StructVector::GetEntries(vec.buffer->Cast<ShreddedVectorBuffer>().GetChild())[1];
}

Vector &ShreddedVector::GetShreddedVector(Vector &vec) {
	VerifyShreddedVector(vec);
	return StructVector::GetEntries(vec.buffer->Cast<ShreddedVectorBuffer>().GetChild())[1];
}

void ShreddedVector::Unshred(const Vector &vec, idx_t count) {
	Vector unshredded_vector(LogicalType::VARIANT(), MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
	auto &shredded_buffer = vec.buffer->Cast<ShreddedVectorBuffer>();
	VariantUtils::UnshredVariantData(shredded_buffer.GetChild(), unshredded_vector, count);
	vec.ConstReference(unshredded_vector);
}

void ShreddedVector::Unshred(const Vector &vec, const SelectionVector &sel, idx_t count) {
	VerifyShreddedVector(vec);
	// slice the underlying shredded buffer
	auto &shredded_buffer = vec.buffer->Cast<ShreddedVectorBuffer>();
	Vector sliced_shredded_buffer(shredded_buffer.GetChild(), sel, count);
	// unshred the vector
	Vector unshredded_vector(LogicalType::VARIANT());
	VariantUtils::UnshredVariantData(sliced_shredded_buffer, unshredded_vector, count);
	vec.ConstReference(unshredded_vector);
}

bool ShreddedVector::IsFullyShredded(Vector &vec) {
	auto &unshredded_vector = GetUnshreddedVector(vec);
	if (unshredded_vector.GetVectorType() == VectorType::CONSTANT_VECTOR && ConstantVector::IsNull(unshredded_vector)) {
		return true;
	}
	return false;
}

} // namespace duckdb
