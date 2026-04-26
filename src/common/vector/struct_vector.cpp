#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/union_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"

namespace duckdb {

VectorStructBuffer::VectorStructBuffer(const LogicalType &type, capacity_t capacity)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER), capacity(capacity) {
	auto &child_types = StructType::GetChildTypes(type);
	for (auto &child_type : child_types) {
		children.emplace_back(child_type.second, capacity);
	}
	validity.Resize(capacity);
}

VectorStructBuffer::VectorStructBuffer(vector<Vector> children_p, capacity_t capacity_p)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER), children(std::move(children_p)),
      capacity(capacity_p) {
	validity.Resize(capacity);
}

VectorStructBuffer::VectorStructBuffer(VectorStructBuffer &other, const SelectionVector &sel, idx_t count)
    : VectorBuffer(VectorType::FLAT_VECTOR, VectorBufferType::STRUCT_BUFFER),
      capacity(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE)) {
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
	v_size = count;
}

void VectorStructBuffer::SetVectorType(VectorType new_vector_type) {
	vector_type = new_vector_type;
	for (auto &child : children) {
		child.SetVectorType(new_vector_type);
	}
}

VectorStructBuffer::~VectorStructBuffer() {
}

void VectorStructBuffer::ResetCapacity(idx_t capacity) {
	this->capacity = capacity;
	validity.Reset(capacity);
}

idx_t VectorStructBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	idx_t size = validity.GetAllocationSize();
	for (auto &child : children) {
		size += child.GetDataSize(count);
	}
	return size;
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
	auto &child_types = StructType::GetChildTypes(type);
	D_ASSERT(child_types.size() == children.size());
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		D_ASSERT(child.GetType() == child_types[child_idx].second);
		child.Verify(sel, count);
		if (vector_type == VectorType::CONSTANT_VECTOR) {
			D_ASSERT(child.GetVectorType() == VectorType::CONSTANT_VECTOR);
			if (!validity.RowIsValid(0)) {
				D_ASSERT(ConstantVector::IsNull(child));
			}
		}
		if (vector_type != VectorType::FLAT_VECTOR) {
			continue;
		}
		// FIXME: re-add struct NULL propagation check
		// for any NULL entry in the struct, the child should be NULL as well
		// this check is currently disabled because projection pushdown and other optimizations
		// may produce structs where parent NULLs are not propagated to all children
	}
}

buffer_ptr<VectorBuffer> VectorStructBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	vector<Vector> result_children;
	for (idx_t i = 0; i < children.size(); i++) {
		result_children.emplace_back(children[i], offset, end);
	}
	auto count = count_t(end - offset);
	auto result = make_buffer<VectorStructBuffer>(std::move(result_children), capacity_t(count));
	result->GetValidityMask().Slice(validity, offset, count);
	result->SetVectorSize(count);
	return result;
}

buffer_ptr<VectorBuffer> VectorStructBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                           idx_t count) {
	return make_buffer<VectorStructBuffer>(*this, sel, count);
}

buffer_ptr<VectorBuffer> VectorStructBuffer::ConstantSliceInternal(const LogicalType &type, count_t count) {
	vector<Vector> result_children;
	for (idx_t i = 0; i < children.size(); i++) {
		result_children.emplace_back(Vector::Ref(children[i]));
	}
	auto result = make_buffer<VectorStructBuffer>(std::move(result_children), capacity_t(1ULL));
	result->SetVectorSize(count);
	result->GetValidityMask().Set(0, validity.RowIsValid(0));
	result->SetVectorType(VectorType::CONSTANT_VECTOR);
	return result;
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

void VectorStructBuffer::Resize(idx_t current_size, idx_t new_size) {
	// resize over the validity
	validity.Resize(new_size);
	// resize the struct children
	for (auto &child : children) {
		child.Resize(current_size, new_size);
	}
	capacity = new_size;
}

void VectorStructBuffer::CopyInternal(const Vector &source, const SelectionVector &source_sel, idx_t source_count,
                                      idx_t source_offset, idx_t target_offset, idx_t copy_count) {
	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == children.size());
	for (idx_t i = 0; i < source_children.size(); i++) {
		children[i].Copy(source_children[i], source_sel, source_count, source_offset, target_offset, copy_count);
	}
}

buffer_ptr<VectorBuffer> VectorStructBuffer::Flatten(const LogicalType &type, idx_t count) const {
	if (GetVectorType() == VectorType::FLAT_VECTOR) {
		for (auto &child : children) {
			child.Flatten(count);
		}
		return nullptr;
	}
	return FlattenSlice(type, *FlatVector::IncrementalSelectionVector(), count);
}

buffer_ptr<VectorBuffer> VectorStructBuffer::FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                                  idx_t count) const {
	// create a new flat struct buffer
	// flatten each child using the same sel
	vector<Vector> result_children;
	for (idx_t i = 0; i < children.size(); i++) {
		result_children.emplace_back(Vector::Ref(children[i]));
		result_children[i].Flatten(sel, count);
	}
	auto result = make_buffer<VectorStructBuffer>(std::move(result_children), capacity_t(count));
	// copy validity using sel
	auto &result_validity = result->GetValidityMask();
	result_validity.CopySel(validity, sel, 0, 0, count);
	result->SetVectorSize(count);
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
	D_ASSERT(vector.GetBufferRef());
	D_ASSERT(vector.Buffer().GetBufferType() == VectorBufferType::STRUCT_BUFFER);
	return vector.BufferMutable().Cast<VectorStructBuffer>().GetChildren();
}

const vector<Vector> &StructVector::GetEntries(const Vector &vector) {
	return GetEntries((Vector &)vector);
}

} // namespace duckdb
