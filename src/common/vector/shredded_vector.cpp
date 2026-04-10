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

idx_t ShreddedVectorBuffer::GetAllocationSize() const {
	idx_t size = VectorBuffer::GetAllocationSize();
	size += shredded_data->GetAllocationSize();
	return size;
}

void ShreddedVectorBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(type.id() == LogicalTypeId::VARIANT);
	D_ASSERT(vector_type == VectorType::SHREDDED_VECTOR);
	shredded_data->Verify(sel, count);
}

string ShreddedVectorBuffer::ToString(const LogicalType &type, idx_t count) const {
	auto &shredded = StructVector::GetEntries(*shredded_data)[1];
	auto &unshredded = StructVector::GetEntries(*shredded_data)[0];
	return "Shredded: " + shredded.ToString(count) + ", Unshredded: " + unshredded.ToString(count);
}

Value ShreddedVectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	// FIXME: this is extremely inefficient
	Vector copy(LogicalType::VARIANT());
	SelectionVector sel(1);
	sel.set_index(0, index);
	auto &shredded = StructVector::GetEntries(*shredded_data)[1];
	auto &unshredded = StructVector::GetEntries(*shredded_data)[0];

	Vector sliced_shredded(shredded, sel, 1);
	Vector sliced_unshredded(unshredded, sel, 1);
	sliced_shredded.Flatten(1);
	sliced_unshredded.Flatten(1);

	child_list_t<LogicalType> shredded_subtypes;
	shredded_subtypes.push_back(make_pair("unshredded", unshredded.GetType()));
	shredded_subtypes.push_back(make_pair("shredded", shredded.GetType()));
	Vector new_shredded(LogicalType::STRUCT(std::move(shredded_subtypes)));
	StructVector::GetEntries(new_shredded)[0].Reference(sliced_unshredded);
	StructVector::GetEntries(new_shredded)[1].Reference(sliced_shredded);

	copy.Shred(new_shredded);
	copy.Flatten(1);
	return copy.GetValue(0);
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
