//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/data_block.hpp
//
//
//===----------------------------------------------------------------------===//

namespace duckdb {
//! The DataBlock is the physical unit to store data it has a physical block which is stored in a file with multiple
//! blocks
class DataBlock {
public:
	DataBlock();
};

//! Stores the header of each data block
struct BlockHeader {
	size_t block_id;
	size_t amount_of_tuples;
};

//! The Block stored in a data block
struct Block {
	BlockHeader *header;
	size_t block_size;  // Block size in Bytes
	size_t offsets[10]; // The offset of each column data TODO: define number of columns based on chunck info
	char *data;
};
} // namespace duckdb
