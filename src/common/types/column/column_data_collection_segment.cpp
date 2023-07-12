#include "duckdb/common/types/column/column_data_collection_segment.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

ColumnDataCollectionSegment::ColumnDataCollectionSegment(shared_ptr<ColumnDataAllocator> allocator_p,
                                                         vector<LogicalType> types_p)
    : allocator(std::move(allocator_p)), types(std::move(types_p)), count(0),
      heap(make_shared<StringHeap>(allocator->GetAllocator())) {
}

idx_t ColumnDataCollectionSegment::GetDataSize(idx_t type_size) {
	return AlignValue(type_size * STANDARD_VECTOR_SIZE);
}

validity_t *ColumnDataCollectionSegment::GetValidityPointer(data_ptr_t base_ptr, idx_t type_size) {
	return reinterpret_cast<validity_t *>(base_ptr + GetDataSize(type_size));
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVectorInternal(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                                    ChunkManagementState *chunk_state) {
	VectorMetaData meta_data;
	meta_data.count = 0;

	auto internal_type = type.InternalType();
	auto type_size = internal_type == PhysicalType::STRUCT ? 0 : GetTypeIdSize(internal_type);
	allocator->AllocateData(GetDataSize(type_size) + ValidityMask::STANDARD_MASK_SIZE, meta_data.block_id,
	                        meta_data.offset, chunk_state);
	if (allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR ||
	    allocator->GetType() == ColumnDataAllocatorType::HYBRID) {
		chunk_meta.block_ids.insert(meta_data.block_id);
	}

	auto index = vector_data.size();
	vector_data.push_back(meta_data);
	return VectorDataIndex(index);
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                            ChunkManagementState *chunk_state,
                                                            VectorDataIndex prev_index) {
	auto index = AllocateVectorInternal(type, chunk_meta, chunk_state);
	if (prev_index.IsValid()) {
		GetVectorData(prev_index).next_data = index;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		// initialize the struct children
		auto &child_types = StructType::GetChildTypes(type);
		auto base_child_index = ReserveChildren(child_types.size());
		for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
			VectorDataIndex prev_child_index;
			if (prev_index.IsValid()) {
				prev_child_index = GetChildIndex(GetVectorData(prev_index).child_index, child_idx);
			}
			auto child_index = AllocateVector(child_types[child_idx].second, chunk_meta, chunk_state, prev_child_index);
			SetChildIndex(base_child_index, child_idx, child_index);
		}
		GetVectorData(index).child_index = base_child_index;
	}
	return index;
}

VectorDataIndex ColumnDataCollectionSegment::AllocateVector(const LogicalType &type, ChunkMetaData &chunk_meta,
                                                            ColumnDataAppendState &append_state,
                                                            VectorDataIndex prev_index) {
	return AllocateVector(type, chunk_meta, &append_state.current_chunk_state, prev_index);
}

VectorDataIndex ColumnDataCollectionSegment::AllocateStringHeap(idx_t size, ChunkMetaData &chunk_meta,
                                                                ColumnDataAppendState &append_state,
                                                                VectorDataIndex prev_index) {
	D_ASSERT(allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	D_ASSERT(size != 0);

	VectorMetaData meta_data;
	meta_data.count = 0;

	allocator->AllocateData(AlignValue(size), meta_data.block_id, meta_data.offset, &append_state.current_chunk_state);
	chunk_meta.block_ids.insert(meta_data.block_id);

	VectorDataIndex index(vector_data.size());
	vector_data.push_back(meta_data);

	if (prev_index.IsValid()) {
		GetVectorData(prev_index).next_data = index;
	}

	return index;
}

void ColumnDataCollectionSegment::AllocateNewChunk() {
	ChunkMetaData meta_data;
	meta_data.count = 0;
	meta_data.vector_data.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		auto vector_idx = AllocateVector(types[i], meta_data);
		meta_data.vector_data.push_back(vector_idx);
	}
	chunk_data.push_back(std::move(meta_data));
}

void ColumnDataCollectionSegment::InitializeChunkState(idx_t chunk_index, ChunkManagementState &state) {
	auto &chunk = chunk_data[chunk_index];
	allocator->InitializeChunkState(state, chunk);
}

VectorDataIndex ColumnDataCollectionSegment::GetChildIndex(VectorChildIndex index, idx_t child_entry) {
	D_ASSERT(index.IsValid());
	D_ASSERT(index.index + child_entry < child_indices.size());
	return VectorDataIndex(child_indices[index.index + child_entry]);
}

VectorChildIndex ColumnDataCollectionSegment::AddChildIndex(VectorDataIndex index) {
	auto result = child_indices.size();
	child_indices.push_back(index);
	return VectorChildIndex(result);
}

VectorChildIndex ColumnDataCollectionSegment::ReserveChildren(idx_t child_count) {
	auto result = child_indices.size();
	for (idx_t i = 0; i < child_count; i++) {
		child_indices.emplace_back();
	}
	return VectorChildIndex(result);
}

void ColumnDataCollectionSegment::SetChildIndex(VectorChildIndex base_idx, idx_t child_number, VectorDataIndex index) {
	D_ASSERT(base_idx.IsValid());
	D_ASSERT(index.IsValid());
	D_ASSERT(base_idx.index + child_number < child_indices.size());
	child_indices[base_idx.index + child_number] = index;
}

idx_t ColumnDataCollectionSegment::ReadVectorInternal(ChunkManagementState &state, VectorDataIndex vector_index,
                                                      Vector &result) {
	auto &vector_type = result.GetType();
	auto internal_type = vector_type.InternalType();
	auto type_size = GetTypeIdSize(internal_type);
	auto &vdata = GetVectorData(vector_index);

	auto base_ptr = allocator->GetDataPointer(state, vdata.block_id, vdata.offset);
	auto validity_data = GetValidityPointer(base_ptr, type_size);
	if (!vdata.next_data.IsValid() && state.properties != ColumnDataScanProperties::DISALLOW_ZERO_COPY) {
		// no next data, we can do a zero-copy read of this vector
		FlatVector::SetData(result, base_ptr);
		FlatVector::Validity(result).Initialize(validity_data);
		return vdata.count;
	}

	// the data for this vector is spread over multiple vector data entries
	// we need to copy over the data for each of the vectors
	// first figure out how many rows we need to copy by looping over all of the child vector indexes
	idx_t vector_count = 0;
	auto next_index = vector_index;
	while (next_index.IsValid()) {
		auto &current_vdata = GetVectorData(next_index);
		vector_count += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	// resize the result vector
	result.Resize(0, vector_count);
	next_index = vector_index;
	// now perform the copy of each of the vectors
	auto target_data = FlatVector::GetData(result);
	auto &target_validity = FlatVector::Validity(result);
	idx_t current_offset = 0;
	while (next_index.IsValid()) {
		auto &current_vdata = GetVectorData(next_index);
		base_ptr = allocator->GetDataPointer(state, current_vdata.block_id, current_vdata.offset);
		validity_data = GetValidityPointer(base_ptr, type_size);
		if (type_size > 0) {
			memcpy(target_data + current_offset * type_size, base_ptr, current_vdata.count * type_size);
		}
		ValidityMask current_validity(validity_data);
		target_validity.SliceInPlace(current_validity, current_offset, 0, current_vdata.count);
		current_offset += current_vdata.count;
		next_index = current_vdata.next_data;
	}
	return vector_count;
}

idx_t ColumnDataCollectionSegment::ReadVector(ChunkManagementState &state, VectorDataIndex vector_index,
                                              Vector &result) {
	auto &vector_type = result.GetType();
	auto internal_type = vector_type.InternalType();
	auto &vdata = GetVectorData(vector_index);
	if (vdata.count == 0) {
		return 0;
	}
	auto vcount = ReadVectorInternal(state, vector_index, result);
	if (internal_type == PhysicalType::LIST) {
		// list: copy child
		auto &child_vector = ListVector::GetEntry(result);
		auto child_count = ReadVector(state, GetChildIndex(vdata.child_index), child_vector);
		ListVector::SetListSize(result, child_count);
	} else if (internal_type == PhysicalType::STRUCT) {
		auto &child_vectors = StructVector::GetEntries(result);
		for (idx_t child_idx = 0; child_idx < child_vectors.size(); child_idx++) {
			auto child_count =
			    ReadVector(state, GetChildIndex(vdata.child_index, child_idx), *child_vectors[child_idx]);
			if (child_count != vcount) {
				throw InternalException("Column Data Collection: mismatch in struct child sizes");
			}
		}
	} else if (internal_type == PhysicalType::VARCHAR) {
		if (allocator->GetType() == ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR) {
			auto next_index = vector_index;
			idx_t offset = 0;
			while (next_index.IsValid()) {
				auto &current_vdata = GetVectorData(next_index);
				for (auto &swizzle_segment : current_vdata.swizzle_data) {
					auto &string_heap_segment = GetVectorData(swizzle_segment.child_index);
					allocator->UnswizzlePointers(state, result, offset + swizzle_segment.offset, swizzle_segment.count,
					                             string_heap_segment.block_id, string_heap_segment.offset);
				}
				offset += current_vdata.count;
				next_index = current_vdata.next_data;
			}
		}
		if (state.properties == ColumnDataScanProperties::DISALLOW_ZERO_COPY) {
			VectorOperations::Copy(result, result, vdata.count, 0, 0);
		}
	}
	return vcount;
}

void ColumnDataCollectionSegment::ReadChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk,
                                            const vector<column_t> &column_ids) {
	D_ASSERT(chunk.ColumnCount() == column_ids.size());
	D_ASSERT(state.properties != ColumnDataScanProperties::INVALID);
	InitializeChunkState(chunk_index, state);
	auto &chunk_meta = chunk_data[chunk_index];
	for (idx_t i = 0; i < column_ids.size(); i++) {
		auto vector_idx = column_ids[i];
		D_ASSERT(vector_idx < chunk_meta.vector_data.size());
		ReadVector(state, chunk_meta.vector_data[vector_idx], chunk.data[i]);
	}
	chunk.SetCardinality(chunk_meta.count);
}

idx_t ColumnDataCollectionSegment::ChunkCount() const {
	return chunk_data.size();
}

idx_t ColumnDataCollectionSegment::SizeInBytes() const {
	D_ASSERT(!allocator->IsShared());
	return allocator->SizeInBytes() + heap->SizeInBytes();
}

void ColumnDataCollectionSegment::FetchChunk(idx_t chunk_idx, DataChunk &result) {
	vector<column_t> column_ids;
	column_ids.reserve(types.size());
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	FetchChunk(chunk_idx, result, column_ids);
}

void ColumnDataCollectionSegment::FetchChunk(idx_t chunk_idx, DataChunk &result, const vector<column_t> &column_ids) {
	D_ASSERT(chunk_idx < chunk_data.size());
	ChunkManagementState state;
	state.properties = ColumnDataScanProperties::DISALLOW_ZERO_COPY;
	ReadChunk(chunk_idx, state, result, column_ids);
}

void ColumnDataCollectionSegment::Verify() {
#ifdef DEBUG
	idx_t total_count = 0;
	for (idx_t i = 0; i < chunk_data.size(); i++) {
		total_count += chunk_data[i].count;
	}
	D_ASSERT(total_count == this->count);
#endif
}

} // namespace duckdb
