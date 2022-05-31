#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

ColumnDataCollection::ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types_p)
    : buffer_manager(buffer_manager), types(move(types_p)), count(0) {
}

ColumnDataCollection::ColumnDataCollection(ClientContext &context, vector<LogicalType> types_p)
    : ColumnDataCollection(BufferManager::GetBufferManager(context), move(types_p)) {
}

ColumnDataCollection::~ColumnDataCollection() {
}

void ColumnDataCollection::CreateInternalData() {
	InternalColumnData data;
	data.count = 0;
	internal_data.push_back(move(data));
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

void ColumnDataCollection::AllocateBlock(InternalColumnData &idata) {
	BlockMetaData data;
	data.size = 0;
	data.capacity = Storage::BLOCK_ALLOC_SIZE;
	data.handle = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	idata.blocks.push_back(move(data));
}

void ColumnDataCollection::AllocateData(InternalColumnData &idata, idx_t size, uint32_t &block_id, uint32_t &offset) {
	if (idata.blocks.empty() || idata.blocks.back().Capacity() < size) {
		AllocateBlock(idata);
	}
	auto &block = idata.blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = idata.blocks.size() - 1;
	offset = block.size;
	block.size += size;
}

idx_t ColumnDataCollection::AllocateVector(InternalColumnData &idata, const LogicalType &type,
                                           ChunkMetaData &chunk_data) {
	VectorMetaData meta_data;
	meta_data.count = 0;

	auto internal_type = type.InternalType();
	if (internal_type == PhysicalType::STRUCT) {
		throw InternalException("FIXME: structs");
	}
	if (internal_type == PhysicalType::LIST) {
		throw InternalException("FIXME: lists");
	}

	auto type_size = GetTypeIdSize(internal_type);
	AllocateData(idata, type_size * STANDARD_VECTOR_SIZE + ValidityMask::STANDARD_MASK_SIZE, meta_data.block_id,
	             meta_data.offset);
	chunk_data.block_ids.insert(meta_data.block_id);
	idata.vector_data.push_back(meta_data);
	return idata.vector_data.size() - 1;
}

void ColumnDataCollection::AllocateNewChunk(InternalColumnData &idata) {
	ChunkMetaData meta_data;
	meta_data.count = 0;
	meta_data.vector_data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		idx_t vector_idx = AllocateVector(idata, types[i], meta_data);
		meta_data.vector_data.push_back(vector_idx);
	}
	idata.chunk_data.push_back(move(meta_data));
}

void ColumnDataCollection::InitializeChunkState(InternalColumnData &idata, idx_t chunk_index,
                                                ChunkManagementState &state) {
	auto &chunk = idata.chunk_data[chunk_index];
	// release any handles that are no longer required
	bool found_handle;
	do {
		found_handle = false;
		for (auto it = state.handles.begin(); it != state.handles.end(); it++) {
			if (chunk.block_ids.find(it->first) != chunk.block_ids.end()) {
				// still required: do not release
				continue;
			}
			state.handles.erase(it);
			found_handle = true;
			break;
		}
	} while (found_handle);

	// grab any handles that are now required
	for (auto &block_id : chunk.block_ids) {
		if (state.handles.find(block_id) != state.handles.end()) {
			// already pinned: don't need to do anything
			continue;
		}
		state.handles[block_id] = buffer_manager.Pin(idata.blocks[block_id].handle);
	}
}

void ColumnDataCollection::InitializeVector(ChunkManagementState &state, VectorMetaData &vdata, Vector &result) {
	auto type_size = GetTypeIdSize(result.GetType().InternalType());

	auto base_ptr = state.handles[vdata.block_id]->Ptr() + vdata.offset;
	auto validity_data = (validity_t *)(base_ptr + type_size * STANDARD_VECTOR_SIZE);
	FlatVector::SetData(result, base_ptr);
	FlatVector::Validity(result).Initialize(validity_data);
}

void ColumnDataCollection::InitializeChunk(InternalColumnData &idata, idx_t chunk_index, ChunkManagementState &state,
                                           DataChunk &chunk) {
	InitializeChunkState(idata, chunk_index, state);
	auto &cdata = idata.chunk_data[chunk_index];
	for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
		InitializeVector(state, idata.vector_data[cdata.vector_data[vector_idx]], chunk.data[vector_idx]);
	}
	chunk.SetCardinality(cdata.count);
}

void ColumnDataCollection::InitializeAppend(ColumnDataAppendState &state) {
	if (internal_data.empty()) {
		CreateInternalData();
	}
	auto &idata = internal_data.back();
	if (idata.chunk_data.empty()) {
		AllocateNewChunk(idata);
	}
	state.append_chunk.InitializeEmpty(types);
	InitializeChunk(idata, idata.chunk_data.size() - 1, state.current_chunk_state, state.append_chunk);
}

void ColumnDataCollection::Append(ColumnDataAppendState &state, DataChunk &new_chunk) {
	D_ASSERT(types == new_chunk.GetTypes());

	auto &idata = internal_data.back();

	idx_t remaining = new_chunk.size();
	while (remaining > 0) {
		auto &cdata = idata.chunk_data.back();
		idx_t append_amount = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE - cdata.count);
		if (append_amount > 0) {
			bool first_append_to_chunk = cdata.count == 0;
			idx_t offset = new_chunk.size() - remaining;
			for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
				if (first_append_to_chunk) {
					// first time appending to this chunk
					// all data here is still uninitialized
					// initialize the validity mask to set all to valid
					auto &validity = FlatVector::Validity(state.append_chunk.data[vector_idx]);
					validity.SetAllValid(STANDARD_VECTOR_SIZE);
				}
				VectorOperations::Copy(new_chunk.data[vector_idx], state.append_chunk.data[vector_idx], append_amount,
				                       offset, cdata.count);
			}
			cdata.count += append_amount;
		}
		remaining -= append_amount;
		if (append_amount > 0) {
			// more to do
			// allocate a new chunk
			AllocateNewChunk(idata);
			InitializeChunk(idata, idata.chunk_data.size() - 1, state.current_chunk_state, state.append_chunk);
		}
	}
}

void ColumnDataCollection::Append(DataChunk &new_chunk) {
	ColumnDataAppendState state;
	InitializeAppend(state);
	Append(state, new_chunk);
}

void ColumnDataCollection::InitializeScan(ColumnDataScanState &state) {
	state.chunk_index = 0;
	state.internal_data_index = 0;
	if (internal_data.empty()) {
		return;
	}
}

void ColumnDataCollection::Scan(ColumnDataScanState &state, DataChunk &result) {
	// check if we still have collections to scan
	if (state.internal_data_index >= internal_data.size()) {
		// no more data left in the scan
		return;
	}
	// check within the current collection if we still have chunks to scan
	while (state.chunk_index >= internal_data[state.internal_data_index].chunk_data.size()) {
		// exhausted all chunks for this internal data structure: move to the next one
		state.internal_data_index++;
		if (state.internal_data_index >= internal_data.size()) {
			return;
		}
	}
	// found a chunk to scan -> scan it
	auto &idata = internal_data[state.internal_data_index];
	InitializeChunk(idata, state.chunk_index, state.current_chunk_state, result);
	state.chunk_index++;
}

void ColumnDataCollection::Combine(ColumnDataCollection &other) {
}

void ColumnDataCollection::Verify() {
}

string ColumnDataCollection::ToString() const {
	return "Column Data Collection";
}

void ColumnDataCollection::Print() const {
	Printer::Print(ToString());
}

idx_t ColumnDataCollection::ChunkCount() const {
	throw InternalException("FIXME: chunk count");
}

void ColumnDataCollection::Reset() {
	count = 0;
	internal_data.clear();
}

} // namespace duckdb
