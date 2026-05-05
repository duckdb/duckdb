#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/types/sel_cache.hpp"

namespace duckdb {

DictionaryBuffer::DictionaryBuffer(const SelectionVector &sel, idx_t sel_count_p, buffer_ptr<DictionaryEntry> entry_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER, count_t(sel_count_p)),
      sel_vector(sel), entry(std::move(entry_p)) {
}
DictionaryBuffer::DictionaryBuffer(buffer_ptr<SelectionData> data, idx_t sel_count_p,
                                   buffer_ptr<DictionaryEntry> entry_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER, count_t(sel_count_p)),
      sel_vector(std::move(data)), entry(std::move(entry_p)) {
}
DictionaryBuffer::DictionaryBuffer(const SelectionVector &sel, idx_t sel_count_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER, count_t(sel_count_p)),
      sel_vector(sel) {
}
DictionaryBuffer::DictionaryBuffer(buffer_ptr<SelectionData> data, idx_t sel_count_p)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER, count_t(sel_count_p)),
      sel_vector(std::move(data)) {
}
DictionaryBuffer::DictionaryBuffer(idx_t count)
    : VectorBuffer(VectorType::DICTIONARY_VECTOR, VectorBufferType::DICTIONARY_BUFFER, count_t(count)),
      sel_vector(count) {
}

idx_t DictionaryBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	// just forward to child node
	return GetEntry().data.GetDataSize(count);
}

idx_t DictionaryBuffer::GetAllocationSize() const {
	auto size = VectorBuffer::GetAllocationSize();
	size += sel_vector.GetAllocationSize();
	return size + GetEntry().data.GetAllocationSize();
}

void DictionaryBuffer::VerifyInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
	D_ASSERT(vector_type == VectorType::DICTIONARY_VECTOR);
	auto &child = GetEntry().data;
	D_ASSERT(type == child.GetType());
	if (!sel.IsSet()) {
		// sel is not set - directly pass in the dictionary
		child.Verify(sel_vector, count);
	} else {
		// sel is set - slice the dictionary with the selection vector
		SelectionVector child_sel(count);
		for (idx_t i = 0; i < count; i++) {
			child_sel.set_index(i, sel_vector.get_index(sel.get_index(i)));
		}
		child.Verify(child_sel, count);
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
	format.validity = FlatVector::ValidityMutable(entry->data);
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
		result = make_buffer<DictionaryBuffer>(cached_dict.GetSelVector(), count, entry);
	} else {
		// no cached entry - perform the slice and store the result
		result = Slice(type, sel, count);
		cache.cache[target_data] = result;
	}
	return result;
}

buffer_ptr<VectorBuffer> DictionaryBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	// dictionary vector slice: slice the dictionary instead of stacking dictionaries
	if (type.InternalType() == PhysicalType::STRUCT) {
		throw InternalException("Struct vectors cannot be dictionary vectors");
	}
	auto count = end - offset;
	auto &sel_data = GetSelVector().sel_data();
	if (!sel_data) {
		// non-owning sel, we need to create a new selection vector to slice
		SelectionVector new_sel(count);
		for (idx_t i = 0; i < count; i++) {
			new_sel.set_index(i, sel_vector.get_index(offset + i));
		}
		return make_uniq<DictionaryBuffer>(new_sel, count, entry);
	}
	if (offset == 0) {
		// for offset = 0 all we have to do is update the count - so just create a new buffer
		return make_uniq<DictionaryBuffer>(sel_data, end, entry);
	}
	SelectionVector sliced_sel(sel_vector.data() + offset, count);
	auto result = make_uniq<DictionaryBuffer>(sliced_sel, count, entry);
	result->AddAuxiliaryData(make_uniq<SelectionDataHolder>(sel_data));
	return result;
}

buffer_ptr<VectorBuffer> DictionaryBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                         idx_t count) {
	// dictionary vector slice: slice the dictionary instead of stacking dictionaries
	if (type.InternalType() == PhysicalType::STRUCT) {
		throw InternalException("Struct vectors cannot be dictionary vectors");
	}
	auto sliced_dictionary = GetSelVector().Slice(sel, count);
	auto new_buffer = make_buffer<DictionaryBuffer>(std::move(sliced_dictionary), count, entry);
	return new_buffer;
}

Value DictionaryBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (index >= Size()) {
		throw InternalException("DictionaryBuffer::GetValue out of range for selection vector");
	}
	auto resolved_index = sel_vector.get_index(index);
	return entry->data.GetValue(resolved_index);
}

buffer_ptr<VectorBuffer> DictionaryBuffer::Flatten(const LogicalType &type) const {
	// flatten the child based on the selection vector stored in the dictionary
	return entry->data.Buffer().FlattenSlice(type, sel_vector, Size());
}

buffer_ptr<VectorBuffer> DictionaryBuffer::FlattenSliceInternal(const LogicalType &type,
                                                                const SelectionVector &input_sel, idx_t count) const {
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
	return entry->data.BufferMutable().FlattenSlice(type, sel, count);
}

buffer_ptr<DictionaryEntry> DictionaryVector::CreateReusableDictionary(const LogicalType &type, const idx_t &size) {
	auto entry = make_buffer<DictionaryEntry>(Vector(type, size));
	FlatVector::SetSize(entry->data, size);
	entry->id = UUID::ToString(UUID::GenerateRandomUUID());
	return entry;
}

const Vector &DictionaryVector::GetCachedHashes(Vector &input) {
	D_ASSERT(CanCacheHashes(input));

	auto &entry = input.BufferMutable().Cast<DictionaryBuffer>().GetEntry();
	lock_guard<mutex> guard(entry.cached_hashes_lock);

	if (!entry.cached_hashes) {
		// Uninitialized: hash the dictionary
		const auto dictionary_size = DictionarySize(input).GetIndex();
		entry.cached_hashes = make_uniq<Vector>(LogicalType::HASH, dictionary_size);
		VectorOperations::Hash(entry.data, *entry.cached_hashes, dictionary_size);
	}
	return *entry.cached_hashes;
}

} // namespace duckdb
