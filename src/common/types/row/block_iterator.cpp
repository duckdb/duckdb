#include "duckdb/common/types/row/block_iterator.hpp"

#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

BlockIteratorStateType GetBlockIteratorStateType(const bool &fixed_blocks, const bool &external) {
	if (!external) {
		return fixed_blocks ? BlockIteratorStateType::FIXED_IN_MEMORY : BlockIteratorStateType::VARIABLE_IN_MEMORY;
	}
	return fixed_blocks ? BlockIteratorStateType::FIXED_EXTERNAL : BlockIteratorStateType::VARIABLE_EXTERNAL;
}

fixed_in_memory_block_iterator_state_t::fixed_in_memory_block_iterator_state_t(const TupleDataCollection &data)
    : block_ptrs(ConvertBlockPointers(data.GetRowBlockPointers())), fast_mod(data.TuplesPerBlock()),
      tuple_count(data.Count()) {
}

unsafe_vector<const data_ptr_t>
fixed_in_memory_block_iterator_state_t::ConvertBlockPointers(const vector<data_ptr_t> &block_ptrs) {
	unsafe_vector<const data_ptr_t> converted_block_ptrs;
	converted_block_ptrs.reserve(block_ptrs.size());
	for (const auto &block_ptr : block_ptrs) {
		converted_block_ptrs.emplace_back(block_ptr);
	}
	return converted_block_ptrs;
}

} // namespace duckdb
