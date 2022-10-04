#include "duckdb/common/types/partitioned_column_data.hpp"

namespace duckdb {

PartitionedColumnData::PartitionedColumnData(ClientContext &context_p, vector<LogicalType> types_p)
    : context(context_p), types(move(types_p)) {
}

PartitionedColumnData::~PartitionedColumnData() {
}

void PartitionedColumnData::InitializeAppendState(PartitionedColumnDataAppendState &state) {
	state.partition_sel.Initialize();
	state.slice_chunk.Initialize(context, types);
	InitializeAppendStateInternal(state);
}

void PartitionedColumnData::AppendChunk(PartitionedColumnDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Compute the counts per partition
	const auto count = input.size();
	unordered_map<idx_t, list_entry_t> partition_entries;
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	for (idx_t i = 0; i < count; i++) {
		const auto &partition_index = partition_indices[i];
		auto partition_entry = partition_entries.find(partition_index);
		if (partition_entry == partition_entries.end()) {
			partition_entries[partition_index] = list_entry_t(0, 1);
		} else {
			partition_entry->second.length++;
		}
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *state.partitions[partition_index];
		auto &partition_append_state = state.partition_append_states[partition_index];
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
		all_partitions_sel[partition_offset++] = i;
	}

	// Loop through the partitions to append the new data to the partition buffers, and flush the buffers if necessary
	SelectionVector partition_sel;
	for (auto &pc : partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, buffer, and append state for this partition index
		auto &partition = *state.partitions[partition_index];
		auto &partition_buffer = *state.partition_buffers[partition_index];
		auto &partition_append_state = state.partition_append_states[partition_index];

		// Length and offset into the selection vector for this chunk, for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Create a selection vector for this partition using the offset into the single selection vector
		partition_sel.Initialize(all_partitions_sel.data() + partition_offset);

		if (partition_length >= 64) {
			// Slice the input chunk using the selection vector
			state.slice_chunk.Reset();
			state.slice_chunk.Slice(input, partition_sel, partition_length);

			// Append it to the partition
			partition.Append(partition_append_state, state.slice_chunk);
		} else {
			// Append the input chunk to the partition buffer using the selection vector
			partition_buffer.Append(input, false, &partition_sel, partition_length);

			if (partition_buffer.size() >= 64) {
				// Next batch won't fit in the buffer, flush it to the partition
				partition.Append(partition_append_state, partition_buffer);
				partition_buffer.Reset(); // TODO: Reset sets the capacity back to STANDARD_VECTOR_SIZE
			}
		}
	}
}

void PartitionedColumnData::CombineLocalState(PartitionedColumnDataAppendState &state) {
	// Flush any remaining data in the buffers
	D_ASSERT(state.partition_buffers.size() == state.partitions.size());
	for (idx_t i = 0; i < state.partitions.size(); i++) {
		auto &partition_buffer = *state.partition_buffers[i];
		if (partition_buffer.size() > 0) {
			state.partitions[i]->Append(partition_buffer);
		}
	}

	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);
	D_ASSERT(state.partitions.size() == NumberOfPartitions());
	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = move(state.partitions);
	} else {
		// Combine the append state's partitions into this PartitionedColumnData
		for (idx_t i = 0; i < NumberOfPartitions(); i++) {
			partitions[i]->Combine(*state.partitions[i]);
		}
	}
}

} // namespace duckdb
