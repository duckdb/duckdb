#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/union_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"

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

VectorStructBuffer::VectorStructBuffer(VectorStructBuffer &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER) {
	auto &other_vector = other.children;
	for (auto &child_vector : other_vector) {
		children.emplace_back(child_vector, sel, count);
	}
	// slice the validity mask of the original struct
	auto &original_validity = other.GetValidityMask();
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

idx_t VectorStructBuffer::GetAllocationSize() const {
	idx_t size = VectorBuffer::GetAllocationSize();
	size += validity.GetAllocationSize();
	for (auto &child : children) {
		size += child.GetAllocationSize();
	}
	return size;
}

void VectorStructBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	if (count == 0) {
		return;
	}
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	D_ASSERT(vector_type == VectorType::FLAT_VECTOR || vector_type == VectorType::CONSTANT_VECTOR);
	if (type.id() == LogicalTypeId::UNION) {
		// FIXME: re-add union vector verification
		// auto valid_check = UnionVector::CheckUnionValidity(vector_p, count, sel_p);
		// if (valid_check != UnionInvalidReason::VALID) {
		// 	throw InternalException("Union not valid, reason: %s", EnumUtil::ToString(valid_check));
		// }
	}
	if (type.id() == LogicalTypeId::VARIANT) {
		// FIXME: re-add variant vector verification
		// if (!VariantUtils::Verify(vector_p, sel_p, count)) {
		// 	throw InternalException("Variant not valid");
		// }
	}
	for (auto &child : children) {
		child.Verify(sel, count);

		// FIXME: re-add this... (don't use .Validity)
		// // for any NULL entry in the struct, the child should be NULL as well
		// auto child_validity = child.Validity(count);
		// for (idx_t i = 0; i < count; i++) {
		// 	auto index = sel.get_index(i);
		// 	if (!validity.RowIsValid(index)) {
		// 		D_ASSERT(!child_validity.IsValid(index));
		// 	}
		// }
	}
}

buffer_ptr<VectorBuffer> VectorStructBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	auto &child_types = StructType::GetChildTypes(type);
	auto result = make_buffer<VectorStructBuffer>();
	auto &result_children = result->GetChildren();
	for (idx_t i = 0; i < children.size(); i++) {
		result_children.emplace_back(child_types[i].second);
		result_children[i].Slice(children[i], offset, end);
	}
	result->GetValidityMask().Slice(validity, offset, end - offset);
	return result;
}

buffer_ptr<VectorBuffer> VectorStructBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                           idx_t count) {
	return make_buffer<VectorStructBuffer>(*this, sel, count);
}

void VectorStructBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
	} else {
		format.sel = FlatVector::IncrementalSelectionVector();
	}
	format.data = nullptr;
	format.validity = validity;
}

void VectorStructBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	validity.Set(index, !val.IsNull());
	if (val.IsNull()) {
		for (auto &child : children) {
			child.SetValue(index, Value());
		}
	} else {
		auto &val_children = StructValue::GetChildren(val);
		D_ASSERT(children.size() == val_children.size());
		for (idx_t i = 0; i < children.size(); i++) {
			children[i].SetValue(index, val_children[i]);
		}
	}
}

Value VectorStructBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	switch (type.id()) {
	case LogicalTypeId::UNION: {
		// tag is the first child
		auto &tag_vector = children[0];
		auto tag_val = tag_vector.GetValue(index);
		if (tag_val.IsNull()) {
			return Value(type);
		}
		auto tag = tag_val.GetValue<union_tag_t>();
		// member is at tag + 1 (tag is child 0)
		auto value = children[tag + 1].GetValue(index);
		auto members = UnionType::CopyMemberTypes(type);
		return Value::UNION(members, tag, std::move(value));
	}
	case LogicalTypeId::VARIANT: {
		duckdb::vector<Value> child_values;
		child_values.emplace_back(children[0].GetValue(index));
		child_values.emplace_back(children[1].GetValue(index));
		child_values.emplace_back(children[2].GetValue(index));
		child_values.emplace_back(children[3].GetValue(index));
		return Value::VARIANT(child_values);
	}
	default: {
		duckdb::vector<Value> child_values;
		for (idx_t i = 0; i < children.size(); i++) {
			child_values.push_back(children[i].GetValue(index));
		}
		if (type.id() == LogicalTypeId::AGGREGATE_STATE) {
			return Value::AGGREGATE_STATE(type, std::move(child_values));
		}
		return Value::STRUCT(type, std::move(child_values));
	}
	}
}

buffer_ptr<VectorBuffer> VectorStructBuffer::Resize(const LogicalType &type, idx_t current_size, idx_t new_size) const {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	// create a new vector struct buffer
	auto result = make_buffer<VectorStructBuffer>();
	// copy over the validity
	result->validity.Resize(new_size);
	if (current_size > 0) {
		result->validity.CopyRange(validity, current_size);
	}
	// resize the struct children
	for (auto &child : children) {
		result->children.emplace_back(Vector::Ref(child));
		result->children.back().Resize(current_size, new_size);
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorStructBuffer::Flatten(const LogicalType &type, const SelectionVector &input_sel,
                                                     idx_t count) const {
	if (!input_sel.IsSet() && GetVectorType() == VectorType::FLAT_VECTOR) {
		for (auto &child : children) {
			child.Flatten(input_sel, count);
		}
		return nullptr;
	}
	// determine the selection vector to use
	SelectionVector owned_sel;
	const_reference<SelectionVector> sel_ref(input_sel);
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// for constant vectors we just use the selection vector [0, 0, 0, 0, 0, 0, ...]
		sel_ref = *ConstantVector::ZeroSelectionVector(count, owned_sel);
	}
	auto &sel = sel_ref.get();

	// create a new flat struct buffer
	auto result = make_buffer<VectorStructBuffer>();
	// copy validity using sel
	auto &result_validity = result->GetValidityMask();
	result_validity.Resize(count);
	result_validity.CopySel(validity, sel, 0, 0, count);

	// flatten each child using the same sel
	auto &result_children = result->GetChildren();
	for (idx_t i = 0; i < children.size(); i++) {
		result_children.emplace_back(Vector::Ref(children[i]));
		result_children[i].Flatten(sel, count);
	}
	return result;
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
