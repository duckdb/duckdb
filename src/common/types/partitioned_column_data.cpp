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

void PartitionedColumnData::Append(PartitionedColumnDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Figure out how many of each partition there are in the input chunk
	const auto count = input.size();
	unordered_map<idx_t, idx_t> partition_counts;
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	for (idx_t i = 0; i < count; i++) {
		partition_counts[partition_indices[i]]++;
	}

	// Now, for each partition, we append to the buffers, and flush the buffers if necessary
	for (auto &pc : partition_counts) {
		const auto &partition_index = pc.first;
		const auto &partition_count = pc.second;
		auto &partition_buffer = *state.partition_buffers[partition_index];

		if (partition_buffer.size() + partition_count > STANDARD_VECTOR_SIZE) {
			// Next batch won't fit in the buffer, flush it to the partition
			state.partitions[partition_index]->Append(partition_buffer);
			partition_buffer.Reset();
		}

		if (partition_count == input.size()) {
			// Whole chunk is a single partition
			partition_buffer.Append(input);
			return;
		}

		// Create a selection vector for this partition
		idx_t sel_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (partition_indices[i] != partition_index) {
				continue;
			}
			state.partition_sel[sel_idx] = i;
			sel_idx++;
		}
		D_ASSERT(sel_idx == partition_count);

		// Append the input chunk to the partition buffer using the selection vector
		partition_buffer.Append(input, false, &state.partition_sel, partition_count);
	}
}

void PartitionedColumnData::AppendLocalState(PartitionedColumnDataAppendState &state) {
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
