#include "duckdb/storage/block.hpp"

namespace duckdb {
using namespace std;

Block::Block(block_id_t id) : FileBuffer(FileBufferType::BLOCK, Storage::BLOCK_ALLOC_SIZE), id(id) {
}

} // namespace duckdb
