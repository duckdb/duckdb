#include "storage/block.hpp"

using namespace duckdb;
using namespace std;

Block::Block(block_id_t id) : FileBuffer(BLOCK_SIZE), id(id) {
}
