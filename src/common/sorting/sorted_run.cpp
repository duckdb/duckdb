#include "duckdb/common/sorting/sorted_run.hpp"

#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "pdqsort.h"

namespace duckdb {

SortedRun::SortedRun(BufferManager &buffer_manager, shared_ptr<TupleDataLayout> key_layout,
                     shared_ptr<TupleDataLayout> payload_layout)
    : key_data(make_uniq<TupleDataCollection>(buffer_manager, std::move(key_layout))),
      payload_data(payload_layout->ColumnCount() != 0
                       ? make_uniq<TupleDataCollection>(buffer_manager, std::move(payload_layout))
                       : nullptr),
      finalized(false) {
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
		key_locations_ptr[i]->payload_ptr = payload_locations_ptr[i];
	}
}

static void SetPayloadPointer(Vector &key_locations, Vector &payload_locations, const idx_t count,
                              const SortKeyType &sort_key_type) {
	switch (sort_key_type) {
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedSetPayloadPointer<SortKeyType::PAYLOAD_FIXED_16>(key_locations, payload_locations, count);
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

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedSort(const TupleDataCollection &key_data) {
	D_ASSERT(SORT_KEY_TYPE == key_data.GetLayout().GetSortKeyType());
	using BLOCK_ITERATOR_STATE = block_iterator_state_t<BlockIteratorStateType::FIXED_IN_MEMORY>;
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	const BLOCK_ITERATOR_STATE state(key_data);
	auto begin = block_iterator_t<const BLOCK_ITERATOR_STATE, SORT_KEY>(state, 0);
	auto end = block_iterator_t<const BLOCK_ITERATOR_STATE, SORT_KEY>(state, key_data.Count());
	duckdb_pdqsort::pdqsort_branchless(begin, end);
}

static void Sort(const TupleDataCollection &key_data) {
	const auto sort_key_type = key_data.GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_8>(key_data);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_16>(key_data);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_32>(key_data);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_VARIABLE_32>(key_data);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_16>(key_data);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_32>(key_data);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::PAYLOAD_VARIABLE_32>(key_data);
	default:
		throw NotImplementedException("TemplatedSort for %s", EnumUtil::ToString(sort_key_type));
	}
}

template <SortKeyType SORT_KEY_TYPE>
static void TemplatedReorderPayload(TupleDataCollection &key_data, TupleDataCollection &payload_data) {
	throw NotImplementedException("Sort");
}

static void ReorderPayload(TupleDataCollection &key_data, TupleDataCollection &payload_data) {
	const auto sort_key_type = key_data.GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedReorderPayload<SortKeyType::PAYLOAD_FIXED_16>(key_data, payload_data);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedReorderPayload<SortKeyType::PAYLOAD_FIXED_32>(key_data, payload_data);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedReorderPayload<SortKeyType::PAYLOAD_VARIABLE_32>(key_data, payload_data);
	default:
		throw NotImplementedException("TemplatedReorderPayload for %s", EnumUtil::ToString(sort_key_type));
	}
}

void SortedRun::Finalize(bool external) {
	D_ASSERT(!finalized);
	key_data->FinalizePinState(key_append_state.pin_state);
	key_data->VerifyEverythingPinned();
	if (payload_data) {
		D_ASSERT(key_data->Count() == payload_data->Count());
		payload_data->FinalizePinState(payload_append_state.pin_state);
		payload_data->VerifyEverythingPinned();
	}

	Sort(*key_data);

	if (external) {
		const auto sort_key_type = key_data->GetLayout().GetSortKeyType();
		if (!SortKeyUtils::IsConstantSize(sort_key_type)) {
			throw NotImplementedException("Sort"); // TODO reorder sort key heap
		}
		key_data->Unpin();
		if (payload_data) {
			ReorderPayload(*key_data, *payload_data);
			payload_data->Unpin();
		}
		// TODO maybe reorder sort key heap and payload in one go
		//  reordering sort key heap is tricky because we don't want to move the row data
		//  reordering payload is easy because we're just reconstructing an entire collection
	}

	finalized = true;
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
