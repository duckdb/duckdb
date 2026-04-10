#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

DictionaryBuffer::DictionaryBuffer(const SelectionVector &sel, buffer_ptr<DictionaryEntry> entry_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel),
      entry(std::move(entry_p)) {
}
DictionaryBuffer::DictionaryBuffer(buffer_ptr<SelectionData> data, buffer_ptr<DictionaryEntry> entry_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER), sel_vector(std::move(data)),
      entry(std::move(entry_p)) {
}
DictionaryBuffer::DictionaryBuffer(const SelectionVector &sel)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER), sel_vector(sel) {
}
DictionaryBuffer::DictionaryBuffer(buffer_ptr<SelectionData> data)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER), sel_vector(std::move(data)) {
}
DictionaryBuffer::DictionaryBuffer(idx_t count)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER), sel_vector(count) {
}

idx_t DictionaryBuffer::GetAllocationSize() const {
	auto size = VectorBuffer::GetAllocationSize();
	size += sel_vector.GetAllocationSize();
	return size + GetEntry().data.GetAllocationSize();
}

void DictionaryBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	if (count == 0) {
		return;
	}
	D_ASSERT(vector_type == VectorType::DICTIONARY_VECTOR);
	if (!sel.IsSet()) {
		// sel is not set - directly pass in the dictionary
		GetEntry().data.Verify(sel_vector, count);
	} else {
		// sel is set - slice the dictionary with the selection vector
		SelectionVector child_sel(count);
		for (idx_t i = 0; i < count; i++) {
			child_sel.set_index(i, sel_vector.get_index(sel.get_index(i)));
		}
		GetEntry().data.Verify(child_sel, count);
	}
}

void DictionaryBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	format.owned_sel.Initialize(sel_vector);
	format.sel = &format.owned_sel;

	auto &child = entry->data;
	if (child.GetVectorType() != VectorType::FLAT_VECTOR) {
		// flatten the child in-place
		entry->data.Flatten(count);
	}
	format.data = FlatVector::GetData(entry->data);
	format.validity = FlatVector::Validity(entry->data);
}

void DictionaryBuffer::Slice(Vector &vector, const SelectionVector &sel, idx_t count) {
	auto dictionary_size_val = dictionary_size;
	auto dictionary_id_val = dictionary_id;
	if (entry) {
		if (!entry->id.empty()) {
			dictionary_id_val = entry->id;
		}
		if (entry->size.IsValid()) {
			dictionary_size_val = entry->size;
		}
	}
	auto sliced_dictionary = sel_vector.Slice(sel, count);
	auto entry_ptr = GetEntryPtr();
	vector.buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary), std::move(entry_ptr));
	if (dictionary_size_val.IsValid()) {
		auto &dict_buffer = vector.buffer->Cast<DictionaryBuffer>();
		dict_buffer.SetDictionarySize(dictionary_size_val.GetIndex());
		dict_buffer.SetDictionaryId(std::move(dictionary_id_val));
	}
}

Value DictionaryBuffer::GetValue(const LogicalType &type, idx_t index) const {
	auto resolved_index = sel_vector.get_index(index);
	return entry->data.GetValue(resolved_index);
}

buffer_ptr<VectorBuffer> DictionaryBuffer::Flatten(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	// determine the effective selection vector
	const SelectionVector *effective_sel = &sel_vector;
	SelectionVector composed;
	if (sel.IsSet()) {
		// compose the provided selection vector with the dictionary selection vector
		composed.Initialize(count);
		for (idx_t i = 0; i < count; i++) {
			composed.set_index(i, sel_vector.get_index(sel.get_index(i)));
		}
		effective_sel = &composed;
	}
	// ensure the child is flat before applying the selection
	auto dict_size = dictionary_size.IsValid() ? dictionary_size.GetIndex()
	                                           : (entry->size.IsValid() ? entry->size.GetIndex() : count);
	entry->data.Flatten(dict_size);
	// copy the now-flat child's data using the effective selection
	return entry->data.GetBuffer()->Flatten(type, *effective_sel, count);
}

buffer_ptr<DictionaryEntry> DictionaryVector::CreateReusableDictionary(const LogicalType &type, const idx_t &size) {
	auto entry = make_buffer<DictionaryEntry>(Vector(type, size));
	entry->size = size;
	entry->id = UUID::ToString(UUID::GenerateRandomUUID());
	return entry;
}

const Vector &DictionaryVector::GetCachedHashes(Vector &input) {
	D_ASSERT(CanCacheHashes(input));

	auto &entry = input.buffer->Cast<DictionaryBuffer>().GetEntry();
	lock_guard<mutex> guard(entry.cached_hashes_lock);

	if (!entry.cached_hashes) {
		// Uninitialized: hash the dictionary
		const auto dictionary_size = DictionarySize(input).GetIndex();
		D_ASSERT(!entry.size.IsValid() || entry.size.GetIndex() == dictionary_size);
		entry.cached_hashes = make_uniq<Vector>(LogicalType::HASH, dictionary_size);
		VectorOperations::Hash(entry.data, *entry.cached_hashes, dictionary_size);
	}
	return *entry.cached_hashes;
}

} // namespace duckdb
