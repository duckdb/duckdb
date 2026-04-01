#include "duckdb/common/vector/list_vector.hpp"

namespace duckdb {

VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, unique_ptr<Vector> vector,
                                   idx_t child_capacity)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), child(std::move(vector)), capacity(child_capacity) {
	if (capacity > 0) {
		auto allocated_data = allocator.Allocate(capacity * sizeof(list_entry_t));
		data_ptr = allocated_data.get();
		AddAuxiliaryData(make_uniq<AuxiliaryAllocatedDataHolder>(std::move(allocated_data)));
	}
}
VectorListBuffer::VectorListBuffer(Allocator &allocator, idx_t capacity, const LogicalType &list_type,
                                   idx_t child_capacity)
    : VectorListBuffer(allocator, capacity, make_uniq<Vector>(ListType::GetChildType(list_type), child_capacity),
                       child_capacity) {
}

VectorListBuffer::VectorListBuffer(idx_t capacity, const LogicalType &list_type, idx_t child_capacity)
    : VectorListBuffer(Allocator::DefaultAllocator(), capacity, list_type, child_capacity) {
}

VectorListBuffer::VectorListBuffer(data_ptr_t data, const VectorListBuffer &parent)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), data_ptr(data), capacity(parent.capacity), size(parent.size) {
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
}

VectorListBuffer::VectorListBuffer(AllocatedData allocated_data_p, const VectorListBuffer &parent)
    : VectorBuffer(VectorBufferType::LIST_BUFFER), capacity(parent.capacity), size(parent.size) {
	child = make_uniq<Vector>(Vector::Ref(parent.GetChild()));
	data_ptr = allocated_data_p.get();
	AddAuxiliaryData(make_uniq<AuxiliaryAllocatedDataHolder>(std::move(allocated_data_p)));
}

void VectorListBuffer::Reserve(idx_t to_reserve) {
	if (to_reserve > capacity) {
		if (to_reserve > DConstants::MAX_VECTOR_SIZE) {
			// overflow: throw an exception
			throw OutOfRangeException("Cannot resize vector to %d rows: maximum allowed vector size is %s", to_reserve,
			                          StringUtil::BytesToHumanReadableString(DConstants::MAX_VECTOR_SIZE));
		}
		idx_t new_capacity = NextPowerOfTwo(to_reserve);
		D_ASSERT(new_capacity >= to_reserve);
		child->Resize(capacity, new_capacity);
		capacity = new_capacity;
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
	while (size + 1 > capacity) {
		child->Resize(capacity, capacity * 2);
		capacity *= 2;
	}
	child->SetValue(size++, insert);
}

void VectorListBuffer::SetCapacity(idx_t new_capacity) {
	this->capacity = new_capacity;
}

void VectorListBuffer::SetSize(idx_t new_size) {
	this->size = new_size;
}

VectorListBuffer::~VectorListBuffer() {
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
	return vec.buffer->Cast<VectorListBuffer>().GetCapacity();
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.buffer = other.buffer;
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
		total_count += entry.value.length;
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
		if (!entry.is_valid) {
			continue;
		}
		auto &list_val = entry.value;
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
		if (!entry.is_valid) {
			continue;
		}
		auto &list_val = entry.value;
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
		if (!list_entry.is_valid) {
			continue;
		}
		auto &list_val = list_entry.value;
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
