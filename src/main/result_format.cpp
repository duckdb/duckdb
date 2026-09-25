#include "duckdb/main/result_format.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/buffered_data/buffered_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

ResultFormatGlobalState::~ResultFormatGlobalState() {
}

ResultFormatLocalState::~ResultFormatLocalState() {
}

ResultFormat::~ResultFormat() {
}

bool ResultFormat::IsChunk() const {
	return StringUtil::Equals(Name(), ChunkFormat::NAME);
}

const shared_ptr<ResultFormat> &ResultFormat::Chunk() {
	return ChunkFormat::InMemory();
}

//===--------------------------------------------------------------------===//
// ChunkFormat
//===--------------------------------------------------------------------===//
class ChunkFormatLocalState : public ResultFormatLocalState {
public:
	unique_ptr<ChunkUnit> unit;
};

ChunkFormat::ChunkFormat(QueryResultMemoryType memory_type_p) : memory_type(memory_type_p) {
}

const shared_ptr<ResultFormat> &ChunkFormat::InMemory() {
	static const shared_ptr<ResultFormat> format = make_shared_ptr<ChunkFormat>(QueryResultMemoryType::IN_MEMORY);
	return format;
}

const shared_ptr<ResultFormat> &ChunkFormat::BufferManaged() {
	static const shared_ptr<ResultFormat> format = make_shared_ptr<ChunkFormat>(QueryResultMemoryType::BUFFER_MANAGED);
	return format;
}

QueryResultMemoryType ChunkFormat::MemoryType() const {
	return memory_type;
}

unique_ptr<ColumnDataCollection> ChunkFormat::CreateCollection(ClientContext &context,
                                                               const vector<LogicalType> &types) const {
	switch (memory_type) {
	case QueryResultMemoryType::IN_MEMORY:
		return make_uniq<ColumnDataCollection>(Allocator::DefaultAllocator(), types);
	case QueryResultMemoryType::BUFFER_MANAGED:
		// The database's buffer manager, because the result can outlive the ClientContext
		return make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(*context.db), types,
		                                       ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	default:
		throw NotImplementedException("ChunkFormat::CreateCollection for %s", EnumUtil::ToString(memory_type));
	}
}

unique_ptr<BatchedDataCollection> ChunkFormat::CreateBatchedCollection(ClientContext &context,
                                                                       vector<LogicalType> types) const {
	switch (memory_type) {
	case QueryResultMemoryType::IN_MEMORY:
		return make_uniq<BatchedDataCollection>(context, std::move(types));
	case QueryResultMemoryType::BUFFER_MANAGED:
		return make_uniq<BatchedDataCollection>(context, std::move(types),
		                                        ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR,
		                                        ColumnDataCollectionLifetime::THROW_ERROR_AFTER_DATABASE_CLOSES);
	default:
		throw NotImplementedException("ChunkFormat::CreateBatchedCollection for %s", EnumUtil::ToString(memory_type));
	}
}

const char *ChunkFormat::Name() const {
	return NAME;
}

unique_ptr<ResultFormatGlobalState> ChunkFormat::InitGlobal(const ResultFormatContext &context) {
	return make_uniq<ResultFormatGlobalState>();
}

unique_ptr<ResultFormatLocalState> ChunkFormat::InitLocal(ResultFormatGlobalState &gstate) {
	return make_uniq<ChunkFormatLocalState>();
}

void ChunkFormat::AppendToUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p, DataChunk &chunk) {
	auto &lstate = lstate_p.Cast<ChunkFormatLocalState>();
	D_ASSERT(!lstate.unit);
	// Copied outside the buffer's lock, so parallel producers copy concurrently
	lstate.unit = make_uniq<ChunkUnit>(BufferedData::CopyForBuffering(chunk));
}

bool ChunkFormat::IsUnitFinished(ResultFormatLocalState &lstate_p) {
	auto &lstate = lstate_p.Cast<ChunkFormatLocalState>();
	return lstate.unit != nullptr;
}

unique_ptr<ResultUnit> ChunkFormat::FinishUnit(ResultFormatGlobalState &gstate, ResultFormatLocalState &lstate_p) {
	auto &lstate = lstate_p.Cast<ChunkFormatLocalState>();
	return std::move(lstate.unit);
}

unique_ptr<DataChunk> ChunkFormat::UnpackUnit(unique_ptr<ResultUnit> unit) {
	if (!unit) {
		return nullptr;
	}
	auto &chunk_unit = unit->Cast<ChunkUnit>();
	for (idx_t i = 0; i < chunk_unit.chunk->ColumnCount(); i++) {
		// CopyForBuffering copies into a freshly initialized chunk, so every column is already flat
		D_ASSERT(chunk_unit.chunk->data[i].GetVectorType() == VectorType::FLAT_VECTOR);
	}
	return std::move(chunk_unit.chunk);
}

} // namespace duckdb
