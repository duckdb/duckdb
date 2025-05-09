#include "duckdb/common/types/row/block_iterator.hpp"

namespace duckdb {

BlockIteratorStateType GetBlockIteratorStateType(const bool &external) {
	return external ? BlockIteratorStateType::EXTERNAL : BlockIteratorStateType::IN_MEMORY;
}

InMemoryBlockIteratorState::InMemoryBlockIteratorState(const TupleDataCollection &key_data)
    : block_ptrs(ConvertBlockPointers(key_data.GetRowBlockPointers())), fast_mod(key_data.TuplesPerBlock()),
      tuple_count(key_data.Count()) {
}

unsafe_vector<data_ptr_t> InMemoryBlockIteratorState::ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs) {
	unsafe_vector<data_ptr_t> converted_block_ptrs;
	converted_block_ptrs.reserve(block_ptrs.size());
	for (const auto &block_ptr : block_ptrs) {
		converted_block_ptrs.emplace_back(block_ptr);
	}
	return converted_block_ptrs;
}

ExternalBlockIteratorState::ExternalBlockIteratorState(TupleDataCollection &key_data_p,
                                                       optional_ptr<TupleDataCollection> payload_data_p)
    : tuple_count(key_data_p.Count()), current_chunk_idx(DConstants::INVALID_INDEX), key_data(key_data_p),
      key_ptrs(FlatVector::GetData<data_ptr_t>(key_scan_state.chunk_state.row_locations)), payload_data(payload_data_p),
      keep_pinned(false), pin_payload(false) {
	key_data.InitializeScan(key_scan_state);
	if (payload_data) {
		payload_data->InitializeScan(payload_scan_state);
	}
}

} // namespace duckdb
