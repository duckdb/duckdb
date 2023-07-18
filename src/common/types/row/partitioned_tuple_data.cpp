#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, BufferManager &buffer_manager_p,
                                           const TupleDataLayout &layout_p)
    : type(type_p), buffer_manager(buffer_manager_p), layout(layout_p.Copy()),
      allocators(make_shared<PartitionTupleDataAllocators>()) {
}

PartitionedTupleData::PartitionedTupleData(const PartitionedTupleData &other)
    : type(other.type), buffer_manager(other.buffer_manager), layout(other.layout.Copy()) {
}

PartitionedTupleData::~PartitionedTupleData() {
}

PartitionedTupleDataType PartitionedTupleData::GetType() const {
	return type;
}

void PartitionedTupleData::InitializeAppendState(PartitionedTupleDataAppendState &state,
                                                 TupleDataPinProperties properties) const {
	state.partition_sel.Initialize();

	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}

	InitializeAppendStateInternal(state, properties);
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, input.size());

	// Early out: check if everything belongs to a single partition
	const auto &partition_entries = state.partition_entries;
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.Append(partition_pin_state, state.chunk_state, input);
		return;
	}

	TupleDataCollection::ToUnifiedFormat(state.chunk_state, input);

	// Compute the heap sizes for the whole chunk
	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(state.chunk_state, input, state.partition_sel, input.size());
	}

	// Build the buffer space
	BuildBufferSpace(state);

	// Now scatter everything in one go
	partitions[0]->Scatter(state.chunk_state, input, state.partition_sel, input.size());
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, TupleDataChunkState &input, idx_t count) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(input.row_locations, count, state.partition_indices);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, count);

	// Early out: check if everything belongs to a single partition
	auto &partition_entries = state.partition_entries;
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		state.chunk_state.heap_sizes.Reference(input.heap_sizes);
		partition.Build(partition_pin_state, state.chunk_state, 0, count);
		partition.CopyRows(state.chunk_state, input, *FlatVector::IncrementalSelectionVector(), count);
		return;
	}

	// Build the buffer space
	state.chunk_state.heap_sizes.Slice(input.heap_sizes, state.partition_sel, count);
	state.chunk_state.heap_sizes.Flatten(count);
	BuildBufferSpace(state);

	// Copy the rows
	partitions[0]->CopyRows(state.chunk_state, input, state.partition_sel, count);
}

void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, idx_t count) {
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	auto &partition_entries = state.partition_entries;
	auto &partition_entries_arr = state.partition_entries_arr;
	partition_entries.clear();

	const auto max_partition_index = MaxPartitionIndex();
	const auto use_arr = max_partition_index < PartitionedTupleDataAppendState::MAP_THRESHOLD;

	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		if (use_arr) {
			std::fill_n(partition_entries_arr, max_partition_index + 1, list_entry_t(0, 0));
			for (idx_t i = 0; i < count; i++) {
				const auto &partition_index = partition_indices[i];
				partition_entries_arr[partition_index].length++;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				const auto &partition_index = partition_indices[i];
				auto partition_entry = partition_entries.find(partition_index);
				if (partition_entry == partition_entries.end()) {
					partition_entries.emplace(partition_index, list_entry_t(0, 1));
				} else {
					partition_entry->second.length++;
				}
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedTupleData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	if (use_arr) {
		for (idx_t partition_index = 0; partition_index <= max_partition_index; partition_index++) {
			auto &partition_entry = partition_entries_arr[partition_index];
			partition_entry.offset = offset;
			offset += partition_entry.length;
		}
	} else {
		for (auto &pc : partition_entries) {
			auto &partition_entry = pc.second;
			partition_entry.offset = offset;
			offset += partition_entry.length;
		}
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &all_partitions_sel = state.partition_sel;
	if (use_arr) {
		for (idx_t i = 0; i < count; i++) {
			const auto &partition_index = partition_indices[i];
			auto &partition_offset = partition_entries_arr[partition_index].offset;
			all_partitions_sel[partition_offset++] = i;
		}
		// Now just add it to the map anyway so the rest of the functionality is shared
		for (idx_t partition_index = 0; partition_index <= max_partition_index; partition_index++) {
			const auto &partition_entry = partition_entries_arr[partition_index];
			if (partition_entry.length != 0) {
				partition_entries.emplace(partition_index, partition_entry);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			const auto &partition_index = partition_indices[i];
			auto &partition_offset = partition_entries[partition_index].offset;
			all_partitions_sel[partition_offset++] = i;
		}
	}
}

void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state) {
	for (auto &pc : state.partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Build out the buffer space for this partition
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
	}
}

void PartitionedTupleData::FlushAppendState(PartitionedTupleDataAppendState &state) {
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

void PartitionedTupleData::Combine(PartitionedTupleData &other) {
	if (other.Count() == 0) {
		return;
	}

	// Now combine the state's partitions into this
	lock_guard<mutex> guard(lock);

	if (partitions.empty()) {
		// This is the first merge, we just copy them over
		partitions = std::move(other.partitions);
	} else {
		D_ASSERT(partitions.size() == other.partitions.size());
		// Combine the append state's partitions into this PartitionedTupleData
		for (idx_t i = 0; i < other.partitions.size(); i++) {
			partitions[i]->Combine(*other.partitions[i]);
		}
	}
}

void PartitionedTupleData::Partition(TupleDataCollection &source, TupleDataPinProperties properties) {
	if (source.Count() == 0) {
		return;
	}
#ifdef DEBUG
	const auto count_before = source.Count();
#endif

	PartitionedTupleDataAppendState append_state;
	InitializeAppendState(append_state, properties);

	TupleDataChunkIterator iterator(source, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
	auto &chunk_state = iterator.GetChunkState();
	do {
		Append(append_state, chunk_state, iterator.GetCurrentChunkCount());
	} while (iterator.Next());

	FlushAppendState(append_state);
	source.Reset();

#ifdef DEBUG
	idx_t count_after = 0;
	for (const auto &partition : partitions) {
		count_after += partition->Count();
	}
	D_ASSERT(count_before == count_after);
#endif
}

void PartitionedTupleData::Repartition(PartitionedTupleData &new_partitioned_data) {
	D_ASSERT(layout.GetTypes() == new_partitioned_data.layout.GetTypes());

	PartitionedTupleDataAppendState append_state;
	new_partitioned_data.InitializeAppendState(append_state);

	const auto reverse = RepartitionReverseOrder();
	const idx_t start_idx = reverse ? partitions.size() : 0;
	const idx_t end_idx = reverse ? 0 : partitions.size();
	const int64_t update = reverse ? -1 : 1;
	const int64_t adjustment = reverse ? -1 : 0;

	for (idx_t partition_idx = start_idx; partition_idx != end_idx; partition_idx += update) {
		auto actual_partition_idx = partition_idx + adjustment;
		auto &partition = *partitions[actual_partition_idx];

		if (partition.Count() > 0) {
			TupleDataChunkIterator iterator(partition, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
			auto &chunk_state = iterator.GetChunkState();
			do {
				new_partitioned_data.Append(append_state, chunk_state, iterator.GetCurrentChunkCount());
			} while (iterator.Next());

			RepartitionFinalizeStates(*this, new_partitioned_data, append_state, actual_partition_idx);
		}
		partitions[actual_partition_idx]->Reset();
	}

	new_partitioned_data.FlushAppendState(append_state);
}

vector<unique_ptr<TupleDataCollection>> &PartitionedTupleData::GetPartitions() {
	return partitions;
}

idx_t PartitionedTupleData::Count() const {
	idx_t total_count = 0;
	for (auto &partition : partitions) {
		total_count += partition->Count();
	}
	return total_count;
}

idx_t PartitionedTupleData::SizeInBytes() const {
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		total_size += partition->SizeInBytes();
	}
	return total_size;
}

void PartitionedTupleData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared<TupleDataAllocator>(buffer_manager, layout));
}

} // namespace duckdb
