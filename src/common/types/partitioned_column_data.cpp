#include "duckdb/common/types/partitioned_column_data.hpp"

namespace duckdb {

PartitionedColumnData::PartitionedColumnData(ClientContext &context_p, vector<LogicalType> types_p)
    : context(context_p), types(move(types_p)) {
}

PartitionedColumnData::~PartitionedColumnData() {
}

void PartitionedColumnData::InitializeAppendState(PartitionedColumnDataAppendState &state) {
	state.partition_sel.Initialize();
	InitializeAppendStateInternal(state);
}

void PartitionedColumnData::AppendChunk(PartitionedColumnDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Compute the counts per partition
	const auto count = input.size();
	map<idx_t, list_entry_t> partition_entries;
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
		auto partition_entry = *partition_entries.begin();
		state.partition_buffers[partition_entry.first]->Append(input);
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
		const auto &partition_entry = pc.second;

		// Length and offset into the selection vector for this chunk, for this partition
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		auto &partition_buffer = *state.partition_buffers[partition_index];
		if (partition_buffer.size() + partition_length > STANDARD_VECTOR_SIZE) {
			// Next batch won't fit in the buffer, flush it to the partition
			auto &partition = *state.partitions[partition_index];
			partition.Append(partition_buffer);
			partition_buffer.Reset();
		}

		// Create a selection vector for this partition using the offset into the single selection vector
		partition_sel.Initialize(all_partitions_sel.data() + partition_offset);

		// Append the input chunk to the partition buffer using the selection vector
		partition_buffer.Append(input, false, &partition_sel, partition_length);
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
		// TODO: sort CDC segments or chunks on their block ID, so we don't read, e.g., 1 chunk per buffer block
	}
}

} // namespace duckdb
