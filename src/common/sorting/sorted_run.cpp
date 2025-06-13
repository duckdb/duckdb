#include "duckdb/common/sorting/sorted_run.hpp"

#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"

#include "vergesort.h"
#include "ska_sort.hpp"

namespace duckdb {

SortedRun::SortedRun(ClientContext &context_p, shared_ptr<TupleDataLayout> key_layout,
                     shared_ptr<TupleDataLayout> payload_layout, bool is_index_sort_p)
    : context(context_p),
      key_data(make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), std::move(key_layout))),
      payload_data(
          payload_layout->ColumnCount() != 0
              ? make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), std::move(payload_layout))
              : nullptr),
      is_index_sort(is_index_sort_p), finalized(false) {
	key_data->InitializeAppend(key_append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	if (payload_data) {
		payload_data->InitializeAppend(payload_append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	}
}

SortedRun::~SortedRun() {
}

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedSetPayloadPointer(Vector &key_locations, Vector &payload_locations, const idx_t count) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;

	const auto key_locations_ptr = FlatVector::GetData<SORT_KEY *>(key_locations);
	const auto payload_locations_ptr = FlatVector::GetData<data_ptr_t>(payload_locations);

	for (idx_t i = 0; i < count; i++) {
		key_locations_ptr[i]->SetPayload(payload_locations_ptr[i]);
	}
}

static void SetPayloadPointer(Vector &key_locations, Vector &payload_locations, const idx_t count,
                              const SortKeyType &sort_key_type) {
	switch (sort_key_type) {
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedSetPayloadPointer<SortKeyType::PAYLOAD_FIXED_16>(key_locations, payload_locations, count);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedSetPayloadPointer<SortKeyType::PAYLOAD_FIXED_24>(key_locations, payload_locations, count);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedSetPayloadPointer<SortKeyType::PAYLOAD_FIXED_32>(key_locations, payload_locations, count);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedSetPayloadPointer<SortKeyType::PAYLOAD_VARIABLE_32>(key_locations, payload_locations, count);
	default:
		throw NotImplementedException("SetPayloadPointer for %s", EnumUtil::ToString(sort_key_type));
	}
}

void SortedRun::Sink(DataChunk &key, DataChunk &payload) {
	D_ASSERT(!finalized);
	key_data->Append(key_append_state, key);
	if (payload_data) {
		D_ASSERT(key.size() == payload.size());
		payload_data->Append(payload_append_state, payload);
		SetPayloadPointer(key_append_state.chunk_state.row_locations, payload_append_state.chunk_state.row_locations,
		                  key.size(), key_data->GetLayout().GetSortKeyType());
	}
}

template <class SORT_KEY>
struct SkaExtractKey {
	using result_type = uint64_t;
	SkaExtractKey(bool requires_next_sort_p, idx_t ska_sort_width_p, const vector<idx_t> &sort_skippable_bytes_p,
	              atomic<bool> &interrupted_p)
	    : requires_next_sort(requires_next_sort_p), ska_sort_width(ska_sort_width_p),
	      sort_skippable_bytes(sort_skippable_bytes_p), interrupted(interrupted_p) {
	}

	const result_type &operator()(const SORT_KEY &key) const {
		return key.part0; // FIXME: this should only be used if there is a part0
	}

	bool ByteIsSkippable(const idx_t &offset) const {
		return std::find(sort_skippable_bytes.begin(), sort_skippable_bytes.end(), offset) !=
		       sort_skippable_bytes.end();
	}

	bool Interrupted() const {
		return interrupted.load(std::memory_order_relaxed);
	}

	bool requires_next_sort;
	idx_t ska_sort_width;
	const vector<idx_t> &sort_skippable_bytes;
	atomic<bool> &interrupted;
};

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedSort(ClientContext &context, const TupleDataCollection &key_data, const bool is_index_sort) {
	const auto &layout = key_data.GetLayout();
	D_ASSERT(SORT_KEY_TYPE == layout.GetSortKeyType());
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCK_ITERATOR_STATE = BlockIteratorState<BlockIteratorStateType::IN_MEMORY>;
	using BLOCK_ITERATOR = block_iterator_t<const BLOCK_ITERATOR_STATE, SORT_KEY>;

	const BLOCK_ITERATOR_STATE state(key_data);
	auto begin = BLOCK_ITERATOR(state, 0);
	auto end = BLOCK_ITERATOR(state, key_data.Count());

	const auto requires_next_sort =
	    is_index_sort ? false : !SORT_KEY::CONSTANT_SIZE || SORT_KEY::INLINE_LENGTH != sizeof(uint64_t);
	const auto ska_sort_width = MinValue<idx_t>(layout.GetSortWidth(), sizeof(uint64_t));
	const auto &sort_skippable_bytes = layout.GetSortSkippableBytes();
	auto ska_extract_key =
	    SkaExtractKey<SORT_KEY>(requires_next_sort, ska_sort_width, sort_skippable_bytes, context.interrupted);

	const auto fallback = [ska_extract_key](const BLOCK_ITERATOR &fb_begin, const BLOCK_ITERATOR &fb_end) {
		duckdb_ska_sort::ska_sort(fb_begin, fb_end, ska_extract_key);
	};
	duckdb_vergesort::vergesort(begin, end, std::less<SORT_KEY>(), fallback);

	if (context.interrupted.load(std::memory_order_relaxed)) {
		throw InterruptException();
	}
}

static void SortSwitch(ClientContext &context, const TupleDataCollection &key_data, bool is_index_sort) {
	const auto sort_key_type = key_data.GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_8>(context, key_data, is_index_sort);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_16>(context, key_data, is_index_sort);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_24>(context, key_data, is_index_sort);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_32>(context, key_data, is_index_sort);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_VARIABLE_32>(context, key_data, is_index_sort);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_16>(context, key_data, is_index_sort);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_24>(context, key_data, is_index_sort);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_32>(context, key_data, is_index_sort);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::PAYLOAD_VARIABLE_32>(context, key_data, is_index_sort);
	default:
		throw NotImplementedException("TemplatedSort for %s", EnumUtil::ToString(sort_key_type));
	}
}

template <class SORT_KEY>
static void ReorderKeyData(TupleDataCollection &new_key_data, TupleDataAppendState &new_key_data_append_state,
                           TupleDataChunkState &input, const idx_t &count) {
	D_ASSERT(!SORT_KEY::CONSTANT_SIZE);
	const auto row_locations = FlatVector::GetData<const SORT_KEY *>(input.row_locations);
	const auto heap_locations = FlatVector::GetData<data_ptr_t>(input.heap_locations);
	const auto heap_sizes = FlatVector::GetData<idx_t>(input.heap_sizes);
	for (idx_t i = 0; i < count; i++) {
		const auto &sort_key = *row_locations[i];
		heap_locations[i] = sort_key.GetData();
		heap_sizes[i] = sort_key.GetHeapSize();
	}

	new_key_data_append_state.chunk_state.heap_sizes.Reference(input.heap_sizes);
	new_key_data.Build(new_key_data_append_state.pin_state, new_key_data_append_state.chunk_state, 0, count);
	new_key_data.CopyRows(new_key_data_append_state.chunk_state, input, *FlatVector::IncrementalSelectionVector(),
	                      count);
}

template <class SORT_KEY>
static void ReorderPayloadData(TupleDataCollection &new_payload_data,
                               TupleDataAppendState &new_payload_data_append_state, SORT_KEY *const *const key_ptrs,
                               TupleDataChunkState &input, const idx_t &count) {
	D_ASSERT(SORT_KEY::HAS_PAYLOAD);
	const auto row_locations = FlatVector::GetData<data_ptr_t>(input.row_locations);
	for (idx_t i = 0; i < count; i++) {
		const auto &sort_key = *key_ptrs[i];
		row_locations[i] = sort_key.GetPayload();
	}

	if (!new_payload_data.GetLayout().AllConstant()) {
		new_payload_data.FindHeapPointers(input, count);
	}
	new_payload_data_append_state.chunk_state.heap_sizes.Reference(input.heap_sizes);
	new_payload_data.Build(new_payload_data_append_state.pin_state, new_payload_data_append_state.chunk_state, 0,
	                       count);
	new_payload_data.CopyRows(new_payload_data_append_state.chunk_state, input,
	                          *FlatVector::IncrementalSelectionVector(), count);
}

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedReorder(ClientContext &context, unique_ptr<TupleDataCollection> &key_data,
                             unique_ptr<TupleDataCollection> &payload_data) {
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	using BLOCK_ITERATOR_STATE = BlockIteratorState<BlockIteratorStateType::IN_MEMORY>;

	// Initialize new key data (if necessary)
	unique_ptr<TupleDataCollection> new_key_data;
	TupleDataAppendState new_key_data_append_state;
	if (!SORT_KEY::CONSTANT_SIZE) {
		new_key_data = key_data->CreateUnique();
		new_key_data->SetPartitionIndex(0); // We'll need the keys before the payload, this keeps them in memory longer
		new_key_data->InitializeAppend(new_key_data_append_state, TupleDataPinProperties::UNPIN_AFTER_DONE);
	}

	// Initialize new payload data (if necessary)
	unique_ptr<TupleDataCollection> new_payload_data;
	TupleDataAppendState new_payload_data_append_state;
	if (SORT_KEY::HAS_PAYLOAD) {
		new_payload_data = payload_data->CreateUnique();
		new_payload_data->InitializeAppend(new_payload_data_append_state, TupleDataPinProperties::UNPIN_AFTER_DONE);
	}

	// These states will be populated for appends
	TupleDataChunkState new_key_data_input;
	TupleDataChunkState new_payload_data_input;
	const auto key_ptrs = FlatVector::GetData<SORT_KEY *>(new_key_data_input.row_locations);

	// Iterate over sort keys
	const idx_t total_count = key_data->Count();
	const BLOCK_ITERATOR_STATE state(*key_data);
	auto it = block_iterator_t<const BLOCK_ITERATOR_STATE, SORT_KEY>(state, 0);

	idx_t index = 0;
	while (index < total_count) {
		if (context.interrupted.load(std::memory_order_relaxed)) {
			throw InterruptException();
		}

		const auto next = MinValue<idx_t>(total_count - index, STANDARD_VECTOR_SIZE);
		for (idx_t i = 0; i < next; i++) {
			key_ptrs[i] = &*it++;
		}
		if (!SORT_KEY::CONSTANT_SIZE) {
			ReorderKeyData<SORT_KEY>(*new_key_data, new_key_data_append_state, new_key_data_input, next);
		}
		if (SORT_KEY::HAS_PAYLOAD) {
			ReorderPayloadData<SORT_KEY>(*new_payload_data, new_payload_data_append_state, key_ptrs,
			                             new_payload_data_input, next);
		}
		index += next;
	}
	D_ASSERT(index == total_count);

	key_data->Unpin();
	if (!SORT_KEY::CONSTANT_SIZE) {
		new_key_data->FinalizePinState(new_key_data_append_state.pin_state);
		key_data = std::move(new_key_data);
	}

	if (SORT_KEY::HAS_PAYLOAD) {
		new_payload_data->FinalizePinState(new_payload_data_append_state.pin_state);
		new_payload_data->Unpin();
		payload_data = std::move(new_payload_data);
	}
}

static void Reorder(ClientContext &context, unique_ptr<TupleDataCollection> &key_data,
                    unique_ptr<TupleDataCollection> &payload_data) {
	const auto sort_key_type = key_data->GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedReorder<SortKeyType::NO_PAYLOAD_VARIABLE_32>(context, key_data, payload_data);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedReorder<SortKeyType::PAYLOAD_FIXED_16>(context, key_data, payload_data);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedReorder<SortKeyType::PAYLOAD_FIXED_24>(context, key_data, payload_data);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedReorder<SortKeyType::PAYLOAD_FIXED_32>(context, key_data, payload_data);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedReorder<SortKeyType::PAYLOAD_VARIABLE_32>(context, key_data, payload_data);
	default:
		throw NotImplementedException("TemplatedReorderPayload for %s", EnumUtil::ToString(sort_key_type));
	}
}

void SortedRun::Finalize(bool external) {
	D_ASSERT(!finalized);

	// Finalize the append
	key_data->FinalizePinState(key_append_state.pin_state);
	key_data->VerifyEverythingPinned();
	if (payload_data) {
		D_ASSERT(key_data->Count() == payload_data->Count());
		payload_data->FinalizePinState(payload_append_state.pin_state);
		payload_data->VerifyEverythingPinned();
	}

	// Sort the fixed-size portion of the keys
	SortSwitch(context, *key_data, is_index_sort);

	if (external) {
		// Reorder variable-size portion of keys and/or payload data (if necessary)
		const auto sort_key_type = key_data->GetLayout().GetSortKeyType();
		if (!SortKeyUtils::IsConstantSize(sort_key_type) || SortKeyUtils::HasPayload(sort_key_type)) {
			Reorder(context, key_data, payload_data);
		}
	}

	finalized = true;
}

void SortedRun::DestroyData(const idx_t tuple_idx_begin, const idx_t tuple_idx_end) {
	// We always have full chunks for sorting, so we can just use the vector size
	const auto chunk_idx_start = tuple_idx_begin / STANDARD_VECTOR_SIZE;
	const auto chunk_idx_end = tuple_idx_end / STANDARD_VECTOR_SIZE;
	if (chunk_idx_start == chunk_idx_end) {
		return;
	}
	key_data->DestroyChunks(chunk_idx_start, chunk_idx_end);
	if (payload_data) {
		payload_data->DestroyChunks(chunk_idx_start, chunk_idx_end);
	}
}

idx_t SortedRun::Count() const {
	return key_data->Count();
}

idx_t SortedRun::SizeInBytes() const {
	idx_t size = key_data->SizeInBytes();
	if (payload_data) {
		size += payload_data->SizeInBytes();
	}
	return size;
}

} // namespace duckdb
