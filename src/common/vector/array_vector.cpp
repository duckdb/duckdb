#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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
	// FIXME: verify validity, arrays have the same validity rules as structs
}

void VectorArrayBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	validity.Set(index, !val.IsNull());
	if (val.IsNull()) {
		for (idx_t i = 0; i < array_size; i++) {
			child->SetValue(index * array_size + i, Value());
		}
	} else {
		auto &val_children = ArrayValue::GetChildren(val);
		for (idx_t i = 0; i < array_size; i++) {
			child->SetValue(index * array_size + i, val_children[i]);
		}
	}
}

Value VectorArrayBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	auto stride = ArrayType::GetSize(type);
	auto offset = index * stride;
	duckdb::vector<Value> children;
	for (idx_t i = offset; i < offset + stride; i++) {
		children.push_back(child->GetValue(i));
	}
	return Value::ARRAY(ArrayType::GetChildType(type), std::move(children));
}

buffer_ptr<VectorBuffer> VectorArrayBuffer::Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (!sel.IsSet() && vector_type == VectorType::FLAT_VECTOR) {
		// already flat - recursively flatten the child vector
		child->Flatten(GetChildSize());
		return nullptr;
	}
	// ensure the child is flat before we access it
	child->Flatten(GetChildSize());

	// determine the selection vector to use
	SelectionVector owned_sel;
	const SelectionVector *active_sel = &sel;
	if (!sel.IsSet()) {
		D_ASSERT(vector_type == VectorType::CONSTANT_VECTOR);
		active_sel = ConstantVector::ZeroSelectionVector(count, owned_sel);
	}
	auto flat_count = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, count);
	auto result = make_buffer<VectorArrayBuffer>(type, flat_count);
	auto &new_child = result->GetChild();

	// copy validity using zero selection vector
	auto &result_validity = result->GetValidityMask();
	result_validity.CopySel(validity, *active_sel, 0, 0, count);

	// unpack the child vector by broadcasting array elements
	SelectionVector child_sel(count * array_size);
	for (idx_t array_idx = 0; array_idx < count; array_idx++) {
		auto src_array_idx = active_sel->get_index(array_idx);
		for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
			auto position = array_idx * array_size + elem_idx;
			auto src_position = src_array_idx * array_size + elem_idx;
			// broadcast the validity
			if (FlatVector::IsNull(*child, src_position)) {
				FlatVector::SetNull(new_child, position, true);
			}
			child_sel.set_index(position, src_position);
		}
	}

	// copy over the data to the new buffer
	VectorOperations::Copy(*child, new_child, child_sel, count * array_size, 0, 0);
	return result;
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
