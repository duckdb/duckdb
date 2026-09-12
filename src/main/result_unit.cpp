#include "duckdb/main/result_unit.hpp"

namespace duckdb {

ResultUnit::ResultUnit(ResultUnitType type_p) : type(type_p) {
}

ResultUnit::~ResultUnit() {
}

ChunkUnit::ChunkUnit(unique_ptr<DataChunk> chunk_p) : ResultUnit(ResultUnitType::CHUNK), chunk(std::move(chunk_p)) {
	D_ASSERT(chunk);
	row_count = chunk->size();
	byte_size = chunk->GetDataSize();
}

} // namespace duckdb
