//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/persistent_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/table/column_segment.hpp"
#include "storage/block.hpp"
#include "storage/block_manager.hpp"

#include "common/unordered_map.hpp"

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BlockManager &manager, block_id_t id, index_t offset, TypeId type, index_t start, index_t count);

	//! The block manager
	BlockManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	index_t offset;
	//! The block that holds the data for this segment (if loaded)
	unique_ptr<Block> block;
	//! The lock to load the block into memory (if not loaded yet)
	std::mutex load_lock;

public:
	void LoadBlock();

	void Scan(ColumnPointer &pointer, Vector &result, index_t count) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) override;
	void Fetch(Vector &result, index_t row_id) override;

private:
	//! Pointer to the dictionary, only used for string blocks
	data_ptr_t dictionary;
	//! Heap used for big strings
	StringHeap heap;
	//! Big string map
	unordered_map<block_id_t, const char *> big_strings;

	void AppendFromStorage(Vector &source, Vector &target, bool has_null);

	template <bool HAS_NULL> void AppendStrings(Vector &source, Vector &target);

	const char *GetBigString(block_id_t block);
};

} // namespace duckdb
