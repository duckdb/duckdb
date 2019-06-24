//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/persistent_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/block.hpp"
#include "storage/table/column_segment.hpp"

namespace duckdb {
class BlockManager;

class PersistentSegment : public ColumnSegment {
public:
	//! Initialize a persistent column segment from a specific block
    PersistentSegment(BlockManager &manager, block_id_t block_id, index_t offset, index_t count, index_t start);

private:
    //! The block manager
    BlockManager &manager;
    //! The block id
    block_id_t block_id;
    //! The offset into the block
    index_t offset;
    //! The data of the column segment
    unique_ptr<Block> block;
    //! The data lock
    std::mutex data_lock;
};

}
