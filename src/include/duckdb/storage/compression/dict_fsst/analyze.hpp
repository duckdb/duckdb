#pragma once

#include "duckdb/storage/compression/dict_fsst/common.hpp"
#include "duckdb/common/primitive_dictionary.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {

namespace dict_fsst {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictFSSTAnalyzeState : public AnalyzeState {
public:
	//! Distinct values the set of a simulated block starts out with
	static constexpr idx_t INITIAL_SET_SIZE = 1024;

public:
	explicit DictFSSTAnalyzeState(BlockManager &block_manager);

public:
	bool Analyze(const Vector &input);
	idx_t FinalAnalyze();

	//! Space one block would need to hold the given amount of tuples and dictionary entries
	static idx_t RequiredSpace(idx_t tuple_count, idx_t unique_count, idx_t dict_size, idx_t max_string_length);

	//! Size of the FSST_ONLY layout, which stores every value encoded with a symbol table per block and no
	//! selection buffer. Returns DConstants::INVALID_INDEX when that mode is not reachable.
	idx_t FSSTOnlyEstimate() const;

private:
	//! Account for a tuple that reuses a dictionary index (NULL or a repeated value); false if the block is full
	bool TryAddIndexOnlyTuple();
	//! Account for a value the current block has not seen, flushing the block first if it does not fit
	void AddNewString(const string_t &str, hash_t hash);
	void FlushSimulatedBlock();
	unique_ptr<PrimitiveDictionary<string_t>> MakeSet(idx_t size) const;
	void GrowSet();

public:
	idx_t max_string_length = 0;
	bool contains_nulls = false;
	//! Effective exclusive size limit for plain dictionary encoding.
	idx_t string_size_limit = 0;
	//! Effective exclusive input size limit for worst-case FSST encoding.
	idx_t fsst_string_size_limit = 0;
	//! Flag which disables the usage of FSST if worst-case encoding blowup does not fit the block size.
	bool disable_fsst = false;
	idx_t total_string_length = 0;
	idx_t total_count = 0;

	//! The dictionary is block-local, so the size estimate has to simulate how values pack into blocks:
	//! duplicates that end up in different blocks are stored once per block, not once in total.
	idx_t segment_count = 0;
	idx_t current_tuple_count = 0;
	idx_t current_unique_count = 0;
	idx_t current_dict_size = 0;
	idx_t current_max_string_length = 0;
	//! Bit width of the dictionary indices and the space the block needs in its current state, cached so a tuple
	//! that reuses an index only has to add a packing group now and then instead of recomputing the layout
	bitpacking_width_t current_indices_width = 0;
	idx_t current_required_space = 0;
	//! Whether a simulated block ever held the same value twice. A segment holds one block worth of values, so
	//! this is also whether FSST_ONLY - which requires every value in a segment to be unique - is reachable.
	bool block_had_repeated_value = false;
	//! Set of the distinct values in the current simulated block, doubled when it fills up
	unique_ptr<PrimitiveDictionary<string_t>> current_set;
	//! Number of values 'current_set' was created for
	idx_t current_set_size = 0;
	//! Most distinct strings a block can hold, where 'current_set' stops growing
	idx_t max_set_size = 0;
	//! Owns the memory of the strings in 'current_set'; reset per simulated block to bound memory usage
	StringHeap heap;
};

} // namespace dict_fsst

} // namespace duckdb
