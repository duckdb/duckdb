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

buffer_ptr<VectorBuffer> VectorStructBuffer::Slice(const LogicalType &type, const VectorBuffer &source, idx_t offset,
                                                   idx_t end) {
	auto &src = source.Cast<const VectorStructBuffer>();
	auto &child_types = StructType::GetChildTypes(type);
	auto result = make_buffer<VectorStructBuffer>();
	auto &result_children = result->GetChildren();
	for (idx_t i = 0; i < src.children.size(); i++) {
		result_children.emplace_back(child_types[i].second);
		result_children[i].Slice(src.children[i], offset, end);
	}
	result->GetValidityMask().Slice(src.validity, offset, end - offset);
	return result;
}

void VectorStructBuffer::FindResizeInfos(Vector &vector, duckdb::vector<ResizeInfo> &resize_infos, idx_t multiplier) {
	VectorBuffer::FindResizeInfos(vector, resize_infos, multiplier);
	for (auto &child : children) {
		child.FindResizeInfos(resize_infos, multiplier);
	}
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

buffer_ptr<VectorBuffer> VectorStructBuffer::Slice(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return nullptr;
	}
	return make_buffer<VectorStructBuffer>(*this, sel, count);
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

buffer_ptr<VectorBuffer> VectorStructBuffer::Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (!sel.IsSet() && vector_type == VectorType::FLAT_VECTOR) {
		// already flat - recursively flatten children
		for (auto &child : children) {
			child.Flatten(sel, count);
		}
		return nullptr;
	}
	// determine the selection vector to use
	SelectionVector owned_sel;
	const_reference<SelectionVector> active_sel_ref(sel);
	if (!sel.IsSet()) {
		D_ASSERT(vector_type == VectorType::CONSTANT_VECTOR);
		active_sel_ref = *ConstantVector::ZeroSelectionVector(count, owned_sel);
	}
	auto &active_sel = active_sel_ref.get();
	auto flat_count = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count);
	// create a new flat struct buffer
	auto result = make_buffer<VectorStructBuffer>(type, flat_count);
	// copy validity using sel
	auto &result_validity = result->GetValidityMask();
	result_validity.CopySel(validity, active_sel, 0, 0, count);
	// flatten each child using the same sel
	auto &result_children = result->GetChildren();
	for (idx_t i = 0; i < children.size(); i++) {
		auto child_result = children[i].GetBuffer()->Flatten(children[i].GetType(), active_sel, count);
		if (child_result) {
			Vector tmp(children[i].GetType(), std::move(child_result));
			result_children[i].CopyBuffer(tmp);
		} else {
			result_children[i].CopyBuffer(children[i]);
		}
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
