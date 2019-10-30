#include "storage/block.hpp"

using namespace duckdb;
using namespace std;

Block::Block(block_id_t id) : FileBuffer(FileBufferType::BLOCK, BLOCK_SIZE), id(id) {
}
