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

namespace duckdb {

class PersistentSegment : public ColumnSegment {
public:
	PersistentSegment(BlockManager &manager, block_id_t id, TypeId type, index_t start, index_t count);

	//! The block manager
	BlockManager &manager;
	//! The block id that this segment relates to
	block_id_t block_id;
	//! The block that holds the data for this segment (if loaded)
	unique_ptr<Block> block;
	//! The lock to load the block into memory (if not loaded yet)
	std::mutex load_lock;
public:
	void LoadBlock();

	void Scan(ColumnPointer &pointer, Vector &result, index_t count) override;
	void Scan(ColumnPointer &pointer, Vector &result, index_t count, sel_t *sel_vector, index_t sel_count) override;
	void Fetch(Vector &result, index_t row_id) override;
};

} // namespace duckdb
