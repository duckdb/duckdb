#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

struct VectorMetaData {
	//! Where the vector data lives
	uint32_t block_id;
	uint32_t offset;
	//! The number of entries present in this vector
	uint16_t count;

	//! Child of this vector (used only for lists and structs)
	idx_t child_data = DConstants::INVALID_INDEX;
	//! Next vector entry (in case there is more data - used only in case of children of lists)
	idx_t next_data = DConstants::INVALID_INDEX;
};

struct ChunkMetaData {
	//! The set of vectors of the chunk
	vector<idx_t> vector_data;
	//! The block ids referenced by the chunk
	unordered_set<uint32_t> block_ids;
	//! The number of entries in the chunk
	uint16_t count;
};

struct BlockMetaData {
	//! The underlying block handle
	shared_ptr<BlockHandle> handle;
	//! How much space is currently used within the block
	uint32_t size;
	//! How much space is available in the block
	uint32_t capacity;

	uint32_t Capacity();
};

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
	//! Allocate space for a vector of a specific type in the segment
	idx_t AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data);
	//! Allocate space for a vector during append,
	idx_t AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data, ColumnDataAppendState &append_state);

	void AllocateBlock();
	void AllocateData(idx_t size, uint32_t &block_id, uint32_t &offset);

	void InitializeChunkState(idx_t chunk_index, ChunkManagementState &state);
	void InitializeChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk);

	idx_t InitializeVector(ChunkManagementState &state, idx_t vector_index, Vector &result);
};

struct ColumnDataMetaData;

typedef void (*column_data_copy_function_t)(ColumnDataMetaData &meta_data, const VectorData &source_data,
                                            Vector &source, idx_t source_offset, idx_t copy_count);

struct ColumnDataCopyFunction {
	column_data_copy_function_t function;
	vector<ColumnDataCopyFunction> child_functions;
};

struct ColumnDataMetaData {
	ColumnDataMetaData(ColumnDataCopyFunction &copy_function, ColumnDataCollectionSegment &segment,
	                   ColumnDataAppendState &state, ChunkMetaData &chunk_data, idx_t vector_data_index)
	    : copy_function(copy_function), segment(segment), state(state), chunk_data(chunk_data),
	      vector_data_index(vector_data_index) {
	}
	ColumnDataMetaData(ColumnDataCopyFunction &copy_function, ColumnDataMetaData &parent, idx_t vector_data_index)
	    : copy_function(copy_function), segment(parent.segment), state(parent.state), chunk_data(parent.chunk_data),
	      vector_data_index(vector_data_index) {
	}

	ColumnDataCopyFunction &copy_function;
	ColumnDataCollectionSegment &segment;
	ColumnDataAppendState &state;
	ChunkMetaData &chunk_data;
	idx_t vector_data_index;
	idx_t child_list_size = DConstants::INVALID_INDEX;

	VectorMetaData &GetVectorMetaData() {
		return segment.vector_data[vector_data_index];
	}
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

idx_t ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data,
                                                  ColumnDataAppendState &append_state) {
	idx_t block_count = blocks.size();
	idx_t vector_index = AllocateVector(type, chunk_data);
	if (blocks.size() != block_count) {
		// we allocated a new block for this vector: pin it
		auto &vdata = vector_data[vector_index];
		D_ASSERT(blocks.size() == block_count + 1);
		auto &last_block = blocks.back();
		append_state.current_chunk_state.handles[vdata.block_id] = buffer_manager.Pin(last_block.handle);
	}
	return vector_index;
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

idx_t ColumnDataCollectionSegment::InitializeVector(ChunkManagementState &state, idx_t vector_index, Vector &result) {
	auto &vector_type = result.GetType();
	auto internal_type = vector_type.InternalType();
	auto type_size = GetTypeIdSize(internal_type);
	auto &vdata = vector_data[vector_index];
	if (internal_type == PhysicalType::LIST) {
		// list: copy child
		auto &child_vector = ListVector::GetEntry(result);
		auto child_count = InitializeVector(state, vdata.child_data, child_vector);
		ListVector::SetListSize(result, child_count);
	}

	auto base_ptr = state.handles[vdata.block_id]->Ptr() + vdata.offset;
	auto validity_data = (validity_t *)(base_ptr + type_size * STANDARD_VECTOR_SIZE);
	if (vdata.next_data == DConstants::INVALID_INDEX) {
		// no next data, we can do a zero-copy read of this vector
		FlatVector::SetData(result, base_ptr);
		FlatVector::Validity(result).Initialize(validity_data);
		return vdata.count;
	}

	// the data for this vector is spread over multiple vector data entries
	// we need to copy over the data for each of the vectors
	// first figure out how many rows we need to copy by looping over all of the child vector indexes
	idx_t vector_count = 0;
	idx_t next_index = vector_index;
	while (next_index != DConstants::INVALID_INDEX) {
		auto &current_vdata = vector_data[next_index];
		vector_count += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	// resize the result vector
	result.Resize(0, vector_count);
	next_index = vector_index;
	// now perform the copy of each of the vectors
	auto target_data = FlatVector::GetData(result);
	idx_t current_offset = 0;
	while (next_index != DConstants::INVALID_INDEX) {
		auto &current_vdata = vector_data[next_index];
		base_ptr = state.handles[current_vdata.block_id]->Ptr() + current_vdata.offset;
		validity_data = (validity_t *)(base_ptr + type_size * STANDARD_VECTOR_SIZE);
		memcpy(target_data + current_offset * type_size, base_ptr, current_vdata.count * type_size);
		// FIXME: copy validity
		current_offset += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	return vector_count;
}

void ColumnDataCollectionSegment::InitializeChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk) {
	InitializeChunkState(chunk_index, state);
	auto &chunk_meta = chunk_data[chunk_index];
	for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
		InitializeVector(state, chunk_meta.vector_data[vector_idx], chunk.data[vector_idx]);
	}
	chunk.SetCardinality(chunk_meta.count);
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
		// first time appending to this vector
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

struct StandardValueCopy {
	template <class T>
	static T Operation(ColumnDataMetaData &, T input) {
		return input;
	}
};

struct StringValueCopy {
	template <class T>
	static T Operation(ColumnDataMetaData &meta_data, T input) {
		return input.IsInlined() ? input : meta_data.segment.heap.AddString(input);
	}
};

struct ListValueCopy {
	template <class T>
	static T Operation(ColumnDataMetaData &meta_data, T input) {
		input.offset += meta_data.child_list_size;
		return input;
	}
};

template <class T, class OP>
static void TemplatedColumnDataCopy(ColumnDataMetaData &meta_data, const VectorData &source_data, idx_t source_offset,
                                    idx_t copy_count) {
	auto &append_state = meta_data.state;
	auto &vector_data = meta_data.GetVectorMetaData();
	D_ASSERT(append_state.current_chunk_state.handles.find(vector_data.block_id) !=
	         append_state.current_chunk_state.handles.end());
	auto base_ptr = append_state.current_chunk_state.handles[vector_data.block_id]->Ptr() + vector_data.offset;
	auto validity_data = (validity_t *)(base_ptr + sizeof(T) * STANDARD_VECTOR_SIZE);
	ColumnDataCopyValidity(source_data, validity_data, source_offset, vector_data.count, copy_count);

	auto ldata = (T *)source_data.data;
	auto result_data = (T *)base_ptr;
	for (idx_t i = 0; i < copy_count; i++) {
		auto source_idx = source_data.sel->get_index(source_offset + i);
		if (source_data.validity.RowIsValid(source_idx)) {
			result_data[vector_data.count + i] = OP::Operation(meta_data, ldata[source_idx]);
		}
	}
	vector_data.count += copy_count;
}

template <class T>
static void ColumnDataCopy(ColumnDataMetaData &meta_data, const VectorData &source_data, Vector &source,
                           idx_t source_offset, idx_t copy_count) {
	TemplatedColumnDataCopy<T, StandardValueCopy>(meta_data, source_data, source_offset, copy_count);
}

template <>
void ColumnDataCopy<string_t>(ColumnDataMetaData &meta_data, const VectorData &source_data, Vector &source,
                              idx_t source_offset, idx_t copy_count) {
	TemplatedColumnDataCopy<string_t, StringValueCopy>(meta_data, source_data, source_offset, copy_count);
}

template <>
void ColumnDataCopy<list_entry_t>(ColumnDataMetaData &meta_data, const VectorData &source_data, Vector &source,
                                  idx_t source_offset, idx_t copy_count) {

	auto &vdata_list = meta_data.segment.vector_data;
	// first append the child entries of the list
	auto &child_vector = ListVector::GetEntry(source);
	idx_t child_list_size = ListVector::GetListSize(source);
	auto &child_type = child_vector.GetType();

	VectorData child_vector_data;
	child_vector.Orrify(child_list_size, child_vector_data);

	if (vdata_list[meta_data.vector_data_index].child_data == DConstants::INVALID_INDEX) {
		vdata_list[meta_data.vector_data_index].child_data =
		    meta_data.segment.AllocateVector(child_type, meta_data.chunk_data, meta_data.state);
	}
	auto &child_function = meta_data.copy_function.child_functions[0];
	idx_t child_index = vdata_list[meta_data.vector_data_index].child_data;

	idx_t remaining = child_list_size;
	idx_t current_list_size = 0;
	while (remaining > 0) {
		current_list_size += meta_data.segment.vector_data[child_index].count;
		idx_t child_append_count =
		    MinValue<idx_t>(STANDARD_VECTOR_SIZE - meta_data.segment.vector_data[child_index].count, remaining);
		if (child_append_count > 0) {
			ColumnDataMetaData child_meta_data(child_function, meta_data, child_index);
			child_function.function(child_meta_data, child_vector_data, child_vector, child_list_size - remaining,
			                        child_append_count);
		}
		remaining -= child_append_count;
		if (remaining > 0) {
			// need to append more, check if we need to allocate a new vector or not
			if (meta_data.segment.vector_data[child_index].next_data == DConstants::INVALID_INDEX) {
				meta_data.segment.vector_data[child_index].next_data =
				    meta_data.segment.AllocateVector(child_type, meta_data.chunk_data, meta_data.state);
			}
			child_index = meta_data.segment.vector_data[child_index].next_data;
		}
	}
	// now copy the list entries
	meta_data.child_list_size = current_list_size;
	TemplatedColumnDataCopy<list_entry_t, ListValueCopy>(meta_data, source_data, source_offset, copy_count);
}

void ColumnDataCopyStruct(ColumnDataMetaData &meta_data, const VectorData &source_data, Vector &source,
                              idx_t source_offset, idx_t copy_count) {
	// copy the NULL values for the main struct vector
	auto &append_state = meta_data.state;
	auto &vector_data = meta_data.GetVectorMetaData();
	D_ASSERT(append_state.current_chunk_state.handles.find(vector_data.block_id) !=
	         append_state.current_chunk_state.handles.end());
	auto base_ptr = append_state.current_chunk_state.handles[vector_data.block_id]->Ptr() + vector_data.offset;
	auto validity_data = (validity_t *) base_ptr;
	ColumnDataCopyValidity(source_data, validity_data, source_offset, vector_data.count, copy_count);

	// now copy all the child vectors
	if (vector_data.child_data == DConstants::INVALID_INDEX) {

	}

}


ColumnDataCopyFunction ColumnDataCollection::GetCopyFunction(const LogicalType &type) {
	ColumnDataCopyFunction result;
	column_data_copy_function_t function;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		function = ColumnDataCopy<bool>;
		break;
	case PhysicalType::INT8:
		function = ColumnDataCopy<int8_t>;
		break;
	case PhysicalType::INT16:
		function = ColumnDataCopy<int16_t>;
		break;
	case PhysicalType::INT32:
		function = ColumnDataCopy<int32_t>;
		break;
	case PhysicalType::INT64:
		function = ColumnDataCopy<int64_t>;
		break;
	case PhysicalType::INT128:
		function = ColumnDataCopy<hugeint_t>;
		break;
	case PhysicalType::UINT8:
		function = ColumnDataCopy<uint8_t>;
		break;
	case PhysicalType::UINT16:
		function = ColumnDataCopy<uint16_t>;
		break;
	case PhysicalType::UINT32:
		function = ColumnDataCopy<uint32_t>;
		break;
	case PhysicalType::UINT64:
		function = ColumnDataCopy<uint64_t>;
		break;
	case PhysicalType::FLOAT:
		function = ColumnDataCopy<float>;
		break;
	case PhysicalType::DOUBLE:
		function = ColumnDataCopy<double>;
		break;
	case PhysicalType::VARCHAR:
		function = ColumnDataCopy<string_t>;
		break;
	case PhysicalType::STRUCT: {
		function = ColumnDataCopyStruct;
		auto &child_types = StructType::GetChildTypes(type);
		for(auto &kv : child_types) {
			result.child_functions.push_back(GetCopyFunction(kv.second));
		}
		break;
	}
	case PhysicalType::LIST: {
		function = ColumnDataCopy<list_entry_t>;
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
		auto &chunk_data = segment.chunk_data.back();
		idx_t append_amount = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE - chunk_data.count);
		if (append_amount > 0) {
			idx_t offset = input.size() - remaining;
			for (idx_t vector_idx = 0; vector_idx < types.size(); vector_idx++) {
				ColumnDataMetaData meta_data(copy_functions[vector_idx], segment, state, chunk_data,
				                             chunk_data.vector_data[vector_idx]);
				copy_functions[vector_idx].function(meta_data, state.vector_data[vector_idx], input.data[vector_idx],
				                                    offset, append_amount);
			}
			chunk_data.count += append_amount;
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
