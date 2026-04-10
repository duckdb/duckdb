#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/sel_cache.hpp"

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
	D_ASSERT(type == GetEntry().data.GetType());
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

buffer_ptr<VectorBuffer> DictionaryBuffer::SliceWithCache(SelCache &cache, const LogicalType &type,
                                                          const SelectionVector &sel, idx_t count) {
	// dictionary vector: need to merge dictionaries
	// check if we have a cached entry
	auto target_data = sel_vector.data();
	auto cache_entry = cache.cache.find(target_data);
	buffer_ptr<VectorBuffer> result;
	if (cache_entry != cache.cache.end()) {
		// cached entry exists: use the cached selection vector with our dictionary entry
		auto &cached_dict = cache_entry->second->Cast<DictionaryBuffer>();
		result = make_buffer<DictionaryBuffer>(cached_dict.GetSelVector(), entry);
	} else {
		// no cached entry - perform the slice and store the result
		result = Slice(type, sel, count);
		cache.cache[target_data] = result;
	}
	if (dictionary_size.IsValid()) {
		auto &dict_buffer = result->Cast<DictionaryBuffer>();
		dict_buffer.SetDictionarySize(dictionary_size.GetIndex());
		dict_buffer.SetDictionaryId(std::move(dictionary_id));
	}
	return result;
}

buffer_ptr<VectorBuffer> DictionaryBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                         idx_t count) {
	// dictionary vector slice: slice the dictionary instead of stacking dictionaries
	if (type.InternalType() == PhysicalType::STRUCT) {
		throw InternalException("Struct vectors cannot be dictionary vectors");
	}
	auto dictionary_size = GetDictionarySize();
	auto dictionary_id = GetDictionaryId();
	auto sliced_dictionary = GetSelVector().Slice(sel, count);
	auto entry = GetEntryPtr();
	auto new_buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary), std::move(entry));
	if (dictionary_size.IsValid()) {
		auto &dict_buffer = new_buffer->Cast<DictionaryBuffer>();
		dict_buffer.SetDictionarySize(dictionary_size.GetIndex());
		dict_buffer.SetDictionaryId(std::move(dictionary_id));
	}
	return new_buffer;
}

Value DictionaryBuffer::GetValue(const LogicalType &type, idx_t index) const {
	auto resolved_index = sel_vector.get_index(index);
	return entry->data.GetValue(resolved_index);
}

buffer_ptr<VectorBuffer> DictionaryBuffer::Flatten(const LogicalType &type, const SelectionVector &input_sel,
                                                   idx_t count) const {
	// get the selection vector to push into the child
	// if input_sel is set, we slice the dictionary by input_sel, otherwise we pass in the dict directly
	const_reference<SelectionVector> sel_ref(sel_vector);
	SelectionVector composed;
	if (input_sel.IsSet()) {
		// slice the dictionary using the provided selection vector
		composed.Initialize(count);
		for (idx_t i = 0; i < count; i++) {
			composed.set_index(i, sel_vector.get_index(input_sel.get_index(i)));
		}
		sel_ref = composed;
	}
	auto &sel = sel_ref.get();

	// flatten the child using the selection vector
	return entry->data.buffer->Flatten(type, sel, count);
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
