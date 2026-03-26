#include "duckdb/common/vector/list_vector.hpp"

namespace duckdb {

template <class T>
T &ListVector::GetEntryInternal(T &vector) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST || vector.GetType().id() == LogicalTypeId::MAP);
	if (vector.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vector);
		return ListVector::GetEntry(child);
	}
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	return vector.auxiliary->template Cast<VectorListBuffer>().GetChild();
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
	D_ASSERT(vector.auxiliary);
	D_ASSERT(vector.auxiliary->GetBufferType() == VectorBufferType::LIST_BUFFER);
	auto &child_buffer = vector.auxiliary->Cast<VectorListBuffer>();
	child_buffer.Reserve(required_capacity);
}

idx_t ListVector::GetListSize(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetSize();
}

idx_t ListVector::GetListCapacity(const Vector &vec) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		return ListVector::GetListSize(child);
	}
	D_ASSERT(vec.auxiliary);
	return vec.auxiliary->Cast<VectorListBuffer>().GetCapacity();
}

void ListVector::ReferenceEntry(Vector &vector, Vector &other) {
	D_ASSERT(vector.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(vector.GetVectorType() == VectorType::FLAT_VECTOR ||
	         vector.GetVectorType() == VectorType::CONSTANT_VECTOR);
	D_ASSERT(other.GetType().id() == LogicalTypeId::LIST);
	D_ASSERT(other.GetVectorType() == VectorType::FLAT_VECTOR || other.GetVectorType() == VectorType::CONSTANT_VECTOR);
	vector.auxiliary = other.auxiliary;
}

void ListVector::SetListSize(Vector &vec, idx_t size) {
	if (vec.GetVectorType() == VectorType::DICTIONARY_VECTOR) {
		auto &child = DictionaryVector::Child(vec);
		ListVector::SetListSize(child, size);
		return;
	}
	vec.auxiliary->Cast<VectorListBuffer>().SetSize(size);
}

void ListVector::Append(Vector &target, const Vector &source, idx_t source_size, idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, source_size, source_offset);
}

void ListVector::Append(Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size,
                        idx_t source_offset) {
	if (source_size - source_offset == 0) {
		//! Nothing to add
		return;
	}
	auto &target_buffer = target.auxiliary->Cast<VectorListBuffer>();
	target_buffer.Append(source, sel, source_size, source_offset);
}

void ListVector::PushBack(Vector &target, const Value &insert) {
	auto &target_buffer = target.auxiliary.get()->Cast<VectorListBuffer>();
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
	for (auto entry : list.ScanValues<list_entry_t>(count)) {
		total_count += entry.value.length;
	}
	return total_count;
}

ConsecutiveChildListInfo ListVector::GetConsecutiveChildListInfo(Vector &list, idx_t offset, idx_t count) {
	ConsecutiveChildListInfo info;
	auto list_data = list.ScanAllValues<list_entry_t>(offset + count);

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
	auto list_data = list.ScanAllValues<list_entry_t>(offset + count);

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
