#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

class ColumnDataCollectionSegment {
public:
	ColumnDataCollectionSegment(BufferManager &buffer_manager, vector<LogicalType> types_p)
	    : buffer_manager(buffer_manager), types(move(types_p)) {
	}

	BufferManager &buffer_manager;
	//! The types of the chunks
	vector<LogicalType> types;
	//! The number of entries in the internal column data
	idx_t count;
	//! The set of blocks used by the column data collection
	vector<BlockMetaData> blocks;
	//! Set of chunk meta data
	vector<ChunkMetaData> chunk_data;
	//! Set of vector meta data
	vector<VectorMetaData> vector_data;
	//! The string heap for the column data collection
	StringHeap heap;

public:
	void AllocateNewChunk();
	idx_t AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data);

	void AllocateBlock();
	void AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset);

	void InitializeChunkState(idx_t chunk_index, ChunkManagementState &state);
	void InitializeChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk);

	void InitializeVector(ChunkManagementState &state, VectorMetaData &vdata, Vector &result);
};

struct ColumnDataCopyFunction;

typedef void (*column_data_copy_function_t)(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
                                            ColumnDataAppendState &state, const VectorData &source_data, Vector &source,
                                            VectorMetaData &target, idx_t source_offset, idx_t target_offset,
                                            idx_t copy_count);

struct ColumnDataCopyFunction {
	column_data_copy_function_t function;
	vector<ColumnDataCopyFunction> child_functions;
};

ColumnDataCollection::ColumnDataCollection(BufferManager &buffer_manager, vector<LogicalType> types_p)
    : buffer_manager(buffer_manager), types(move(types_p)), count(0) {
	copy_functions.reserve(types.size());
	for (auto &type : types) {
		copy_functions.push_back(GetCopyFunction(type));
	}
}

ColumnDataCollection::ColumnDataCollection(ClientContext &context, vector<LogicalType> types_p)
    : ColumnDataCollection(BufferManager::GetBufferManager(context), move(types_p)) {
}

ColumnDataCollection::~ColumnDataCollection() {
}

void ColumnDataCollection::CreateSegment() {
	ColumnDataCollectionSegment segment(buffer_manager, types);
	segment.count = 0;
	segments.push_back(move(segment));
}

uint32_t BlockMetaData::Capacity() {
	D_ASSERT(size <= capacity);
	return capacity - size;
}

void ColumnDataCollectionSegment::AllocateBlock() {
	BlockMetaData data;
	data.size = 0;
	data.capacity = Storage::BLOCK_ALLOC_SIZE;
	data.handle = buffer_manager.RegisterMemory(Storage::BLOCK_ALLOC_SIZE, false);
	blocks.push_back(move(data));
}

void ColumnDataCollectionSegment::AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset) {
	if (blocks.empty() || blocks.back().Capacity() < size) {
		AllocateBlock();
	}
	auto &block = blocks.back();
	D_ASSERT(size <= block.capacity - block.size);
	block_id = blocks.size() - 1;
	offset = block.size;
	block.size += size;
}

idx_t ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data) {
	VectorMetaData meta_data;
	meta_data.count = 0;

	auto internal_type = type.InternalType();
	auto type_size = internal_type == PhysicalType::STRUCT ? 0 : GetTypeIdSize(internal_type);
	AllocateData(type_size * STANDARD_VECTOR_SIZE + ValidityMask::STANDARD_MASK_SIZE, meta_data.block_id,
	             meta_data.offset);
	chunk_data.block_ids.insert(meta_data.block_id);

	auto index = vector_data.size();
	vector_data.push_back(meta_data);
	return index;
}

void ColumnDataCollectionSegment::AllocateNewChunk() {
	ChunkMetaData meta_data;
	meta_data.count = 0;
	meta_data.vector_data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		idx_t vector_idx = AllocateVector(types[i], meta_data);
		meta_data.vector_data.push_back(vector_idx);
	}
	chunk_data.push_back(move(meta_data));
}

void ColumnDataCollectionSegment::InitializeChunkState(idx_t chunk_index, ChunkManagementState &state) {
	auto &chunk = chunk_data[chunk_index];
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
		state.handles[block_id] = buffer_manager.Pin(blocks[block_id].handle);
	}
}

void ColumnDataCollectionSegment::InitializeVector(ChunkManagementState &state, VectorMetaData &vdata, Vector &result) {
	auto type_size = GetTypeIdSize(result.GetType().InternalType());

	auto base_ptr = state.handles[vdata.block_id]->Ptr() + vdata.offset;
	auto validity_data = (validity_t *)(base_ptr + type_size * STANDARD_VECTOR_SIZE);
	FlatVector::SetData(result, base_ptr);
	FlatVector::Validity(result).Initialize(validity_data);
}

void ColumnDataCollectionSegment::InitializeChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk) {
	InitializeChunkState(chunk_index, state);
	auto &cdata = chunk_data[chunk_index];
	for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
		InitializeVector(state, vector_data[cdata.vector_data[vector_idx]], chunk.data[vector_idx]);
	}
	chunk.SetCardinality(cdata.count);
}

void ColumnDataCollection::InitializeAppend(ColumnDataAppendState &state) {
	state.vector_data.resize(types.size());
	if (segments.empty()) {
		CreateSegment();
	}
	auto &segment = segments.back();
	if (segment.chunk_data.empty()) {
		segment.AllocateNewChunk();
	}
	segment.InitializeChunkState(segment.chunk_data.size() - 1, state.current_chunk_state);
}

void ColumnDataCopyValidity(const VectorData &source_data, validity_t *target, idx_t source_offset, idx_t target_offset,
                            idx_t copy_count) {
	ValidityMask validity(target);
	if (target_offset == 0) {
		// first time appending to this chunk
		// all data here is still uninitialized
		// initialize the validity mask to set all to valid
		validity.SetAllValid(STANDARD_VECTOR_SIZE);
	}
	// FIXME: we can do something more optimized here using bitshifts & bitwise ors
	if (!source_data.validity.AllValid()) {
		for (idx_t i = 0; i < copy_count; i++) {
			auto idx = source_data.sel->get_index(source_offset + i);
			if (!source_data.validity.RowIsValid(idx)) {
				validity.SetInvalid(target_offset + i);
			}
		}
	}
}

template <class T>
static void TemplatedColumnDataCopy(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
                                    ColumnDataAppendState &state, const VectorData &source_data, Vector &source,
                                    VectorMetaData &target, idx_t source_offset, idx_t target_offset,
                                    idx_t copy_count) {
	auto base_ptr = state.current_chunk_state.handles[target.block_id]->Ptr() + target.offset;
	auto validity_data = (validity_t *)(base_ptr + sizeof(T) * STANDARD_VECTOR_SIZE);
	ColumnDataCopyValidity(source_data, validity_data, source_offset, target_offset, copy_count);

	auto ldata = (T *)source_data.data;
	auto result_data = (T *)base_ptr;
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = source_data.sel->get_index(source_offset + i);
		result_data[target_offset + i] = ldata[source_idx];
	}
}

template <>
void TemplatedColumnDataCopy<string_t>(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
                                       ColumnDataAppendState &state, const VectorData &source_data, Vector &source,
                                       VectorMetaData &target, idx_t source_offset, idx_t target_offset,
                                       idx_t copy_count) {
	auto base_ptr = state.current_chunk_state.handles[target.block_id]->Ptr() + target.offset;
	auto validity_data = (validity_t *)(base_ptr + sizeof(string_t) * STANDARD_VECTOR_SIZE);
	ColumnDataCopyValidity(source_data, validity_data, source_offset, target_offset, copy_count);

	auto ldata = (string_t *)source_data.data;
	auto result_data = (string_t *)base_ptr;
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = source_data.sel->get_index(source_offset + i);
		result_data[target_offset + i] =
		    ldata[source_idx].IsInlined() ? ldata[source_idx] : segment.heap.AddString(ldata[source_idx]);
	}
}

template <>
void TemplatedColumnDataCopy<list_entry_t>(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
                                           ColumnDataAppendState &state, const VectorData &source_data, Vector &source,
                                           VectorMetaData &target, idx_t source_offset, idx_t target_offset,
                                           idx_t copy_count) {
	auto base_ptr = state.current_chunk_state.handles[target.block_id]->Ptr() + target.offset;
	auto validity_data = (validity_t *)(base_ptr + sizeof(list_entry_t) * STANDARD_VECTOR_SIZE);
	ColumnDataCopyValidity(source_data, validity_data, source_offset, target_offset, copy_count);

	auto ldata = (list_entry_t *)source_data.data;
	auto result_data = (list_entry_t *)base_ptr;
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = source_data.sel->get_index(source_offset + i);
		result_data[target_offset + i] = ldata[source_idx];
	}

	VectorData child_data;
	auto &child_vector = ListVector::GetEntry(source);
	auto child_size = ListVector::GetListSize(source);
	child_vector.Orrify(child_size, child_data);

	throw InternalException("FIXME: list append");
	//
	//	D_ASSERT(target.child_data < idata.vector_data.size());
	//	if (target.child_data == DConstants::INVALID_INDEX) {
	//
	//	}
	//	auto &child_meta_data = idata.vector_data[target.child_data];
	//
	//	auto &child_function = copy_function.child_functions[0];
	//	child_function.function(child_function, idata, state, child_data, child_vector, child_meta_data, 0,
	// child_meta_data.count, child_size);
}

ColumnDataCopyFunction ColumnDataCollection::GetCopyFunction(const LogicalType &type) {
	ColumnDataCopyFunction result;
	column_data_copy_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = TemplatedColumnDataCopy<bool>;
		break;
	case PhysicalType::INT8:
		function = TemplatedColumnDataCopy<int8_t>;
		break;
	case PhysicalType::INT16:
		function = TemplatedColumnDataCopy<int16_t>;
		break;
	case PhysicalType::INT32:
		function = TemplatedColumnDataCopy<int32_t>;
		break;
	case PhysicalType::INT64:
		function = TemplatedColumnDataCopy<int64_t>;
		break;
	case PhysicalType::INT128:
		function = TemplatedColumnDataCopy<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = TemplatedColumnDataCopy<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = TemplatedColumnDataCopy<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = TemplatedColumnDataCopy<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = TemplatedColumnDataCopy<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = TemplatedColumnDataCopy<float>;
		break;
	case PhysicalType::DOUBLE:
		function = TemplatedColumnDataCopy<double>;
		break;
	case PhysicalType::VARCHAR:
		function = TemplatedColumnDataCopy<string_t>;
		break;
	case PhysicalType::LIST: {
		function = TemplatedColumnDataCopy<list_entry_t>;
		auto child_function = GetCopyFunction(ListType::GetChildType(type));
		result.child_functions.push_back(child_function);
		break;
	}
	default:
		throw InternalException("Unsupported type for ColumnDataCollection::GetCopyFunction");
	}
	result.function = function;
	return result;
}

void ColumnDataCollection::Append(ColumnDataAppendState &state, DataChunk &input) {
	D_ASSERT(types == input.GetTypes());

	auto &segment = segments.back();
	for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
		input.data[vector_idx].Orrify(input.size(), state.vector_data[vector_idx]);
	}

	idx_t remaining = input.size();
	while (remaining > 0) {
		auto &cdata = segment.chunk_data.back();
		idx_t append_amount = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE - cdata.count);
		if (append_amount > 0) {
			idx_t offset = input.size() - remaining;
			for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
				copy_functions[vector_idx].function(
				    copy_functions[vector_idx], segment, state, state.vector_data[vector_idx], input.data[vector_idx],
				    segment.vector_data[cdata.vector_data[vector_idx]], offset, cdata.count, append_amount);
			}
			cdata.count += append_amount;
		}
		remaining -= append_amount;
		if (remaining > 0) {
			// more to do
			// allocate a new chunk
			segment.AllocateNewChunk();
			segment.InitializeChunkState(segment.chunk_data.size() - 1, state.current_chunk_state);
		}
	}
	count += input.size();
}

void ColumnDataCollection::Append(DataChunk &input) {
	ColumnDataAppendState state;
	InitializeAppend(state);
	Append(state, input);
}

void ColumnDataCollection::InitializeScan(ColumnDataScanState &state) {
	state.chunk_index = 0;
	state.segment_index = 0;
	state.current_chunk_state.handles.clear();
}

void ColumnDataCollection::Scan(ColumnDataScanState &state, DataChunk &result) {
	result.Reset();

	// check if we still have collections to scan
	if (state.segment_index >= segments.size()) {
		// no more data left in the scan
		return;
	}
	// check within the current collection if we still have chunks to scan
	while (state.chunk_index >= segments[state.segment_index].chunk_data.size()) {
		// exhausted all chunks for this internal data structure: move to the next one
		state.chunk_index = 0;
		state.segment_index++;
		if (state.segment_index >= segments.size()) {
			return;
		}
	}
	// found a chunk to scan -> scan it
	auto &segment = segments[state.segment_index];
	segment.InitializeChunk(state.chunk_index, state.current_chunk_state, result);
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
	segments.clear();
}

} // namespace duckdb
