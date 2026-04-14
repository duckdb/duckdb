#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, unique_ptr<Vector> vector)
    : StandardVectorBuffer(allocator, capacity, sizeof(list_entry_t)), child(std::move(vector)) {
	buffer_type = VectorBufferType::LIST_BUFFER;
}
VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, const LogicalType &list_type,
                                   idx_t child_capacity)
    : VectorListBuffer(allocator, capacity, make_uniq<Vector>(ListType::GetChildType(list_type), child_capacity)) {
}

VectorListBuffer::VectorListBuffer(idx_t capacity, const LogicalType &list_type, idx_t child_capacity)
    : VectorListBuffer(Allocator::DefaultAllocator(), capacity, list_type, child_capacity) {
}

VectorListBuffer::VectorListBuffer(data_ptr_t data, idx_t capacity, const Vector &vector, idx_t child_size)
    : StandardVectorBuffer(data, capacity) {
	buffer_type = VectorBufferType::LIST_BUFFER;
	size = child_size;
	child = make_uniq<Vector>(Vector::Ref(vector));
}

VectorListBuffer::VectorListBuffer(data_ptr_t data, idx_t capacity, const VectorListBuffer &parent)
    : StandardVectorBuffer(data, capacity), size(parent.size) {
	buffer_type = VectorBufferType::LIST_BUFFER;
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
}

VectorListBuffer::VectorListBuffer(AllocatedData allocated_data_p, idx_t capacity, const VectorListBuffer &parent)
    : StandardVectorBuffer(std::move(allocated_data_p), capacity), size(parent.size) {
	buffer_type = VectorBufferType::LIST_BUFFER;
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > GetChildCapacity()) {
		if (to_reserve > DConstants::MAX_VECTOR_SIZE) {
			// overflow: throw an exception
			throw OutOfRangeException("Cannot resize vector to %d rows: maximum allowed vector size is %s", to_reserve,
			                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
		}
		idx_t new_capacity = NextPowerOfTwo(to_reserve);
		D_ASSERT(new_capacity >= to_reserve);
		child->Resize(size, new_capacity);
	}
}

void VectorListBuffer::Append(const Vector &to_append, idx_t to_append_size, idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::Append(const Vector &to_append, const SelectionVector &sel, idx_t to_append_size,
                              idx_t source_offset) {
	Reserve(size + to_append_size - source_offset);
	VectorOperations::Copy(to_append, *child, sel, to_append_size, source_offset, size);
	size += to_append_size - source_offset;
}

void VectorListBuffer::PushBack(const Value &insert) {
	auto child_capacity = GetChildCapacity();
	while (size + 1 > child_capacity) {
		child->Resize(child_capacity, child_capacity * 2);
		child_capacity *= 2;
	}
	child->SetValue(size++, insert);
}

idx_t VectorListBuffer::GetChildCapacity() const {
	return FlatVector::GetCapacity(*child);
}

void VectorListBuffer::SetSize(idx_t new_size) {
	this->size = new_size;
}

VectorListBuffer::~VectorListBuffer() {
}

idx_t VectorListBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	idx_t size = StandardVectorBuffer::GetDataSize(type, count);
	size += child->GetDataSize(this->size);
	return size;
}

idx_t VectorListBuffer::GetAllocationSize() const {
	idx_t size = StandardVectorBuffer::GetAllocationSize();
	size += GetChild().GetAllocationSize();
	return size;
}

void VectorListBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	if (count == 0) {
		return;
	}
	D_ASSERT(type.InternalType() == PhysicalType::LIST);
	D_ASSERT(vector_type == VectorType::FLAT_VECTOR || vector_type == VectorType::CONSTANT_VECTOR);
	if (type.id() == LogicalTypeId::MAP) {
		// FIXME: verify map
		// auto &child = ListType::GetChildType(vector_p.GetType());
		// D_ASSERT(StructType::GetChildCount(child) == 2);
		// D_ASSERT(StructType::GetChildName(child, 0) == "key");
		// D_ASSERT(StructType::GetChildName(child, 1) == "value");
		//
		// auto valid_check = MapVector::CheckMapValidity(vector_p, count, sel_p);
		// D_ASSERT(valid_check == MapInvalidReason::VALID);
	}
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		count = 1;
	}
	// NOTE: size > capacity can occur in valid intermediate states (e.g. after SetListSize before Reserve)
	// D_ASSERT(size <= capacity);
	idx_t total_size = 0;
	auto child_size = GetSize();
	auto list_data = reinterpret_cast<list_entry_t *>(data_ptr);
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		idx = vector_type == VectorType::CONSTANT_VECTOR ? 0 : idx;
		auto &le = list_data[idx];
		if (validity.RowIsValid(idx)) {
			D_ASSERT(le.offset + le.length <= child_size);
			total_size += le.length;
		}
	}
	SelectionVector child_sel(total_size);
	idx_t child_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = sel.get_index(i);
		idx = vector_type == VectorType::CONSTANT_VECTOR ? 0 : idx;
		auto &le = list_data[idx];
		if (validity.RowIsValid(idx)) {
			D_ASSERT(le.offset + le.length <= child_size);
			for (idx_t k = 0; k < le.length; k++) {
				child_sel.set_index(child_count++, le.offset + k);
			}
		}
	}
	child->Verify(child_sel, child_count);
}

buffer_ptr<VectorBuffer> VectorListBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	auto type_size = GetTypeIdSize(type.InternalType());
	auto offset_ptr = data_ptr + type_size * offset;
	auto result = make_buffer<VectorListBuffer>(offset_ptr, end - offset, *this);
	result->GetValidityMask().Slice(validity, offset, end - offset);
	return result;
}

void VectorListBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		format.sel = ConstantVector::ZeroSelectionVector(count, format.owned_sel);
	} else {
		format.sel = FlatVector::IncrementalSelectionVector();
	}
	format.data = data_ptr;
	format.validity = validity;
}

buffer_ptr<VectorBuffer> VectorListBuffer::CreateBuffer(AllocatedData &&new_data, idx_t capacity) const {
	return make_buffer<VectorListBuffer>(std::move(new_data), capacity, *this);
}

buffer_ptr<VectorBuffer> VectorListBuffer::Flatten(const LogicalType &type, const SelectionVector &sel,
                                                   idx_t count) const {
	auto result = StandardVectorBuffer::Flatten(type, sel, count);
	if (!result) {
		// already flat - flatten the child
		auto &child = GetChild();
		child.Flatten(size);
		return nullptr;
	}
	// created a new buffer - also flatten the child
	auto &list_result = result->Cast<VectorListBuffer>();
	list_result.GetChild().Flatten(list_result.size);
	return result;
}

void VectorListBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (!val.IsNull() && val.type() != type) {
		SetValue(type, index, val.DefaultCastAs(type));
		return;
	}
	validity.Set(index, !val.IsNull());
	auto offset = size;
	if (val.IsNull()) {
		PushBack(Value());
		auto &entry = reinterpret_cast<list_entry_t *>(data_ptr)[index];
		entry.length = 1;
		entry.offset = offset;
	} else {
		auto &val_children = ListValue::GetChildren(val);
		for (idx_t i = 0; i < val_children.size(); i++) {
			PushBack(val_children[i]);
		}
		auto &entry = reinterpret_cast<list_entry_t *>(data_ptr)[index];
		entry.length = val_children.size();
		entry.offset = offset;
	}
}

Value VectorListBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		index = 0;
	}
	if (!validity.RowIsValid(index)) {
		return Value(type);
	}
	auto offlen = reinterpret_cast<const list_entry_t *>(data_ptr)[index];
	duckdb::vector<Value> children;
	for (idx_t i = offlen.offset; i < offlen.offset + offlen.length; i++) {
		children.push_back(child->GetValue(i));
	}
	if (type.id() == LogicalTypeId::MAP) {
		return Value::MAP(ListType::GetChildType(type), std::move(children));
	}
	return Value::LIST(ListType::GetChildType(type), std::move(children));
}

template <class T>
T &ListVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.buffer);
	D_ASSERT(vector.buffer->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return vector.buffer->template Cast<VectorListBuffer>().GetChild();
}

const Vector &ListVector::GetEntry(const Vector &vector) {
	return GetEntryInternal<const Vector>(vector);
}

Vector &ListVector::GetEntry(Vector &vector) {
	return GetEntryInternal<Vector>(vector);
}

void ListVector::Reserve(Vector &vector, idx_t required_capacity) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.buffer);
	D_ASSERT(vector.buffer->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = vector.buffer->Cast<VectorListBuffer>();
	child_buffer.Reserve(required_capacity);
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.buffer);
	return vec.buffer->Cast<VectorListBuffer>().GetSize();
}

idx_t ListVector::GetListCapacity(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		throw InternalException("ListVector::GetListCapacity called on dictionary vector");
	}
	D_ASSERT(vec.buffer);
	return vec.buffer->Cast<VectorListBuffer>().GetChildCapacity();
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		throw InternalException("ListVector::SetListSize called on dictionary vector");
	}
	vec.buffer->Cast<VectorListBuffer>().SetSize(size);
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.buffer->Cast<VectorListBuffer>();
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.buffer->Cast<VectorListBuffer>();
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = target.buffer.get()->Cast<VectorListBuffer>();
	target_buffer.PushBack(insert);
}

idx_t ListVector::GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count) {
	auto info = ListVector::GetConsecutiveChildListInfo(list, offset, count);
	if (info.needs_slicing) {
		SelectionVector sel(info.child_list_info.length);
		ListVector::GetConsecutiveChildSelVector(list, sel, offset, count);

		result.Slice(sel, info.child_list_info.length);
		result.Flatten(info.child_list_info.length);
	}
	return info.child_list_info.length;
}

idx_t ListVector::GetTotalEntryCount(Vector &list, idx_t count) {
	idx_t total_count = 0;
	for (auto entry : list.ValidValues<list_entry_t>(count)) {
		total_count += entry.GetValue().length;
	}
	return total_count;
}

ConsecutiveChildListInfo ListVector::GetConsecutiveChildListInfo(Vector &list, idx_t offset, idx_t count) {
	ConsecutiveChildListInfo info;
	auto list_data = list.Values<list_entry_t>(offset + count);

	// find the first non-NULL entry
	idx_t first_length = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto entry = list_data[i];
		if (!entry.IsValid()) {
			continue;
		}
		auto &list_val = entry.GetValue();
		info.child_list_info.offset = list_val.offset;
		first_length = list_val.length;
		break;
	}

	// small performance improvement for constant vectors
	// avoids iterating over all their (constant) elements
	if (list.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		info.child_list_info.length = first_length;
		return info;
	}

	// now get the child count and determine whether the children are stored consecutively
	// also determine if a flat vector has pseudo constant values (all offsets + length the same)
	// this can happen e.g. for UNNESTs
	bool is_consecutive = true;
	for (idx_t i = offset; i < offset + count; i++) {
		auto entry = list_data[i];
		if (!entry.IsValid()) {
			continue;
		}
		auto &list_val = entry.GetValue();
		if (list_val.offset != info.child_list_info.offset || list_val.length != first_length) {
			info.is_constant = false;
		}
		if (list_val.offset != info.child_list_info.offset + info.child_list_info.length) {
			is_consecutive = false;
		}
		info.child_list_info.length += list_val.length;
	}

	if (info.is_constant) {
		info.child_list_info.length = first_length;
	}
	if (!info.is_constant && !is_consecutive) {
		info.needs_slicing = true;
	}

	return info;
}

void ListVector::GetConsecutiveChildSelVector(Vector &list, SelectionVector &sel, idx_t offset, idx_t count) {
	auto list_data = list.Values<list_entry_t>(offset + count);

	//	SelectionVector child_sel(info.second.length);
	idx_t entry = 0;
	for (idx_t i = offset; i < offset + count; i++) {
		auto list_entry = list_data[i];
		if (!list_entry.IsValid()) {
			continue;
		}
		auto &list_val = list_entry.GetValue();
		for (idx_t k = 0; k < list_val.length; k++) {
			//			child_sel.set_index(entry++, list_data[idx].offset + k);
			sel.set_index(entry++, list_val.offset + k);
		}
	}
	//
	//	result.Slice(child_sel, info.second.length);
	//	result.Flatten(info.second.length);
	//	info.second.offset = 0;
}

} // namespace duckdb
