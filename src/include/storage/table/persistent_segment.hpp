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
#include "storage/buffer_manager.hpp"

#include "common/unordered_map.hpp"

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BufferManager &manager, block_id_t id, index_t offset, TypeId type, index_t start, index_t count);

	//! The block manager
	BufferManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The offset into the block
	index_t offset;
public:
	void Scan(ColumnPointer &pointer, Vector &result, index_t count) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) override;
	void Fetch(Vector &result, index_t row_id) override;
private:
	//! Heap used for big strings
	StringHeap heap;
	//! Lock for big strings
	std::mutex big_string_lock;
	//! Big string map
	unordered_map<block_id_t, const char *> big_strings;

	void AppendFromStorage(Block *block, Vector &source, Vector &target, bool has_null);

	template <bool HAS_NULL> void AppendStrings(Block *block, Vector &source, Vector &target);

	const char *GetBigString(block_id_t block);
};

} // namespace duckdb
