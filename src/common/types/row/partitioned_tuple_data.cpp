#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, ClientContext &context_p,
                                           TupleDataLayout layout_p)
    : type(type_p), context(context_p), layout(std::move(layout_p)),
      allocators(make_shared<PartitionTupleDataAllocators>()) {
}

PartitionedTupleData::PartitionedTupleData(const PartitionedTupleData &other)
    : type(other.type), context(other.context), layout(other.layout) {
}

unique_ptr<PartitionedTupleData> PartitionedTupleData::CreateShared() {
	switch (type) {
	case PartitionedTupleDataType::RADIX:
		return make_unique<RadixPartitionedTupleData>((RadixPartitionedTupleData &)*this);
	default:
		throw NotImplementedException("CreateShared for this type of PartitionedTupleData");
	}
}

PartitionedTupleData::~PartitionedTupleData() {
}

void PartitionedTupleData::InitializeAppendState(PartitionedTupleDataAppendState &state) const {
	state.partition_sel.Initialize();

	vector<column_t> column_ids;
	column_ids.reserve(layout.ColumnCount());
	for (idx_t col_idx = 0; col_idx < layout.ColumnCount(); col_idx++) {
		column_ids.emplace_back(col_idx);
	}

	InitializeAppendStateInternal(state);
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, DataChunk &input) {
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input);

	// Compute the counts per partition
	const auto count = input.size();
	unordered_map<idx_t, list_entry_t> partition_entries;
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
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
		throw InternalException("Unexpected VectorType in PartitionedTupleData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		const auto &partition_index = partition_entries.begin()->first;
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.Append(partition_pin_state, state.chunk_state, input);
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

	TupleDataCollection::ToUnifiedFormat(state.chunk_state, input);

	// Compute the heap sizes for the whole chunk
	if (!layout.AllConstant()) {
		TupleDataCollection::ComputeHeapSizes(state.chunk_state, input, all_partitions_sel, input.size());
	}

	for (auto &pc : partition_entries) {
		const auto &partition_index = pc.first;

		// Partition, and pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = pc.second;
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Build out the buffer space for this partition
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
	}

	// Now scatter everything in one go
	partitions[0]->Scatter(state.chunk_state, input, all_partitions_sel, input.size());
}

void PartitionedTupleData::FlushAppendState(PartitionedTupleDataAppendState &state) {
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

void PartitionedTupleData::Combine(PartitionedTupleData &other) {
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

vector<unique_ptr<TupleDataCollection>> &PartitionedTupleData::GetPartitions() {
	return partitions;
}

void PartitionedTupleData::CreateAllocator() {
	allocators->allocators.emplace_back(
	    make_shared<TupleDataAllocator>(BufferManager::GetBufferManager(context), layout));
}

} // namespace duckdb
