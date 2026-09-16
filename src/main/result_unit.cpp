#include "duckdb/main/result_unit.hpp"

namespace duckdb {

ResultUnit::ResultUnit(idx_t row_count_p, idx_t byte_size_p) : row_count(row_count_p), byte_size(byte_size_p) {
}

ResultUnit::~ResultUnit() {
}

static DataChunk &RequireChunk(const unique_ptr<DataChunk> &chunk) {
	D_ASSERT(chunk);
	return *chunk;
}

ChunkUnit::ChunkUnit(unique_ptr<DataChunk> chunk_p)
    : ResultUnit(RequireChunk(chunk_p).size(), chunk_p->GetDataSize()), chunk(std::move(chunk_p)) {
}

const char *ChunkUnit::TypeTag() const {
	return TAG;
}

} // namespace duckdb
