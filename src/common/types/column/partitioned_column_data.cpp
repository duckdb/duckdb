#include "duckdb/common/types/column/partitioned_column_data.hpp"

#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedColumnData::PartitionedColumnData(PartitionedColumnDataType type_p, ClientContext &context_p,
                                             vector<LogicalType> types_p)
    : type(type_p), context(context_p), types(std::move(types_p)),
      allocators(make_shared_ptr<PartitionColumnDataAllocators>()) {
}

PartitionedColumnData::PartitionedColumnData(const PartitionedColumnData &other)
    : type(other.type), context(other.context), types(other.types), allocators(other.allocators) {
}

unique_ptr<PartitionedColumnData> PartitionedColumnData::CreateShared() {
	switch (type) {
	case PartitionedColumnDataType::RADIX:
		return make_uniq<RadixPartitionedColumnData>(Cast<RadixPartitionedColumnData>());
	default:
		throw NotImplementedException("CreateShared for this type of PartitionedColumnData");
	}
}

PartitionedColumnData::~PartitionedColumnData() {
}

void PartitionedColumnData::InitializeAppendState(PartitionedColumnDataAppendState &state) const {
	state.partition_sel.Initialize();
	state.slice_chunk.Initialize(BufferAllocator::Get(context), types);
	InitializeAppendStateInternal(state);
}

unique_ptr<DataChunk> PartitionedColumnData::CreatePartitionBuffer() const {
	auto result = make_uniq<DataChunk>();
	result->Initialize(BufferAllocator::Get(context), types, BufferSize());
	return result;
}

void PartitionedColumnData::Append(PartitionedColumnDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Compute the counts per partition
	const auto count = input.size();
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	auto &partition_entries = state.partition_entries;
	partition_entries.clear();
	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < count; i++) {
			const auto &partition_index = partition_indices[i];
			auto partition_entry = partition_entries.find(partition_index);
			if (partition_entry == partition_entries.end()) {
				partition_entries[partition_index] = list_entry_t(0, 1);
			} else {
				partition_entry->second.length++;
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedColumnData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_append_state = *state.partition_append_states[partition_index];
		partition.Append(partition_append_state, input);
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	for (auto &pc : partition_entries) {
		auto &partition_entry = pc.second;
		partition_entry.offset = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &all_partitions_sel = state.partition_sel;
	for (idx_t i = 0; i < count; i++) {
		const auto &partition_index = partition_indices[i];
		auto &partition_offset = partition_entries[partition_index].offset;
		all_partitions_sel[partition_offset++] = NumericCast<sel_t>(i);
	}

	// Loop through the partitions to append the new data to the partition buffers, and flush the buffers if necessary
	SelectionVector partition_sel;
	for (auto &pc : partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, buffer, and append state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_buffer = *state.partition_buffers[partition_index];
		auto &partition_append_state = *state.partition_append_states[partition_index];

		// Length and offset into the selection vector for this chunk, for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Create a selection vector for this partition using the offset into the single selection vector
		partition_sel.Initialize(all_partitions_sel.data() + partition_offset);

		if (partition_length >= HalfBufferSize()) {
			// Slice the input chunk using the selection vector
			state.slice_chunk.Reset();
			state.slice_chunk.Slice(input, partition_sel, partition_length);

			// Append it to the partition directly
			partition.Append(partition_append_state, state.slice_chunk);
		} else {
			// Append the input chunk to the partition buffer using the selection vector
			partition_buffer.Append(input, false, &partition_sel, partition_length);

			if (partition_buffer.size() >= HalfBufferSize()) {
				// Next batch won't fit in the buffer, flush it to the partition
				partition.Append(partition_append_state, partition_buffer);
				partition_buffer.Reset();
				partition_buffer.SetCapacity(BufferSize());
			}
		}
	}
}

void PartitionedColumnData::FlushAppendState(PartitionedColumnDataAppendState &state) {
	for (idx_t i = 0; i < state.partition_buffers.size(); i++) {
		if (!state.partition_buffers[i]) {
			continue;
		}
		auto &partition_buffer = *state.partition_buffers[i];
		if (partition_buffer.size() > 0) {
			partitions[i]->Append(partition_buffer);
			partition_buffer.Reset();
		}
	}
}

void PartitionedColumnData::Combine(PartitionedColumnData &other) {
	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);

	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = std::move(other.partitions);
	} else {
		D_ASSERT(partitions.size() == other.partitions.size());
		// Combine the append state's partitions into this PartitionedColumnData
		for (idx_t i = 0; i < other.partitions.size(); i++) {
			if (!other.partitions[i]) {
				continue;
			}
			if (!partitions[i]) {
				partitions[i] = std::move(other.partitions[i]);
			} else {
				partitions[i]->Combine(*other.partitions[i]);
			}
		}
	}
}

vector<unique_ptr<ColumnDataCollection>> &PartitionedColumnData::GetPartitions() {
	return partitions;
}

void PartitionedColumnData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared_ptr<ColumnDataAllocator>(BufferManager::GetBufferManager(context)));
	allocators->allocators.back()->MakeShared();
}

} // namespace duckdb
