#include "duckdb/common/sorting/sorted_run.hpp"

#include "duckdb/common/sorting/sort_key.hpp"
#include "duckdb/common/types/row/block_iterator.hpp"
#include "pdqsort.h"

namespace duckdb {

SortedRun::SortedRun(BufferManager &buffer_manager, const TupleDataLayout &key_layout,
                     const TupleDataLayout &payload_layout)
    : key_data(make_uniq<TupleDataCollection>(buffer_manager, key_layout)),
      payload_data(payload_layout.ColumnCount() != 0 ? make_uniq<TupleDataCollection>(buffer_manager, payload_layout)
                                                     : nullptr),
      finalized(false) {
	key_data->InitializeAppend(key_append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	if (payload_data) {
		payload_data->InitializeAppend(payload_append_state, TupleDataPinProperties::KEEP_EVERYTHING_PINNED);
	}
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
	using SORT_KEY = SortKey<SORT_KEY_TYPE>;
	D_ASSERT(SORT_KEY_TYPE == key_data.GetLayout().GetSortKeyType());
	BlockIteratorState<SORT_KEY> block_iterator_state(key_data.GetRowBlockPointers(), key_data.TuplesPerBlock(),
	                                                  key_data.Count());
	duckdb_pdqsort::pdqsort_branchless(block_iterator_t<SORT_KEY>(block_iterator_state, 0),
	                                   block_iterator_t<SORT_KEY>(block_iterator_state, key_data.Count()));
}

static void Sort(const TupleDataCollection &key_data) {
	const auto sort_key_type = key_data.GetLayout().GetSortKeyType();
	switch (sort_key_type) {
	case SortKeyType::NO_PAYLOAD_FIXED_8:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_8>(key_data);
	case SortKeyType::NO_PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_16>(key_data);
	case SortKeyType::NO_PAYLOAD_FIXED_24:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_24>(key_data);
	case SortKeyType::NO_PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_FIXED_32>(key_data);
	case SortKeyType::NO_PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::NO_PAYLOAD_VARIABLE_32>(key_data);
	case SortKeyType::PAYLOAD_FIXED_16:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_16>(key_data);
	case SortKeyType::PAYLOAD_FIXED_24:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_24>(key_data);
	case SortKeyType::PAYLOAD_FIXED_32:
		return TemplatedSort<SortKeyType::PAYLOAD_FIXED_32>(key_data);
	case SortKeyType::PAYLOAD_VARIABLE_32:
		return TemplatedSort<SortKeyType::PAYLOAD_VARIABLE_32>(key_data);
	default:
		throw NotImplementedException("TemplatedSort for %s", EnumUtil::ToString(sort_key_type));
	}
}

void SortedRun::Finalize(bool external) {
	// Finish appending
	key_data->FinalizePinState(key_append_state.pin_state);
	key_data->VerifyEverythingPinned();
	if (payload_data) {
		payload_data->FinalizePinState(payload_append_state.pin_state);
		payload_data->VerifyEverythingPinned();
	}

	Sort(*key_data);
}

} // namespace duckdb
