#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, BufferManager &buffer_manager_p,
                                           shared_ptr<TupleDataLayout> &layout_ptr_p, MemoryTag tag_p)
    : type(type_p), buffer_manager(buffer_manager_p),
      stl_allocator(make_shared_ptr<ArenaAllocator>(buffer_manager.GetBufferAllocator())), layout_ptr(layout_ptr_p),
      layout(*layout_ptr), tag(tag_p), count(0), data_size(0) {
}

PartitionedTupleData::PartitionedTupleData(PartitionedTupleData &other)
    : PartitionedTupleData(other.type, other.buffer_manager, other.layout_ptr, other.tag) {
}

PartitionedTupleData::~PartitionedTupleData() {
}

shared_ptr<TupleDataLayout> PartitionedTupleData::GetLayoutPtr() const {
	return layout_ptr;
}

const TupleDataLayout &PartitionedTupleData::GetLayout() const {
	return *layout_ptr;
}

PartitionedTupleDataType PartitionedTupleData::GetType() const {
	return type;
}

void PartitionedTupleData::InitializeAppendState(PartitionedTupleDataAppendState &state,
                                                 TupleDataPinProperties properties) const {
	state.partition_sel.Initialize();
	state.reverse_partition_sel.Initialize();

	InitializeAppendStateInternal(state, properties);
}

void PartitionedTupleData::ResetAppendState(PartitionedTupleDataAppendState &state,
                                            TupleDataPinProperties properties) const {
	// Default: fall back to full re-initialization (subclasses can override with a faster path)
	InitializeAppendState(state, properties);
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, DataChunk &input,
                                  const SelectionVector &append_sel, const idx_t append_count) {
	TupleDataCollection::ToUnifiedFormat(state.chunk_state, input);
	AppendUnified(state, input, append_sel, append_count);
}

bool PartitionedTupleData::UseFixedSizeMap() const {
	return MaxPartitionIndex() < PartitionedTupleDataAppendState::MAP_THRESHOLD;
}

void PartitionedTupleData::AppendUnified(PartitionedTupleDataAppendState &state, DataChunk &input,
                                         const SelectionVector &append_sel, const idx_t append_count) {
	const idx_t actual_append_count = append_count == DConstants::INVALID_INDEX ? input.size() : append_count;

	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(state, input, append_sel, actual_append_count);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, append_sel, actual_append_count);

	// Early out: check if everything belongs to a single partition
	const auto partition_index = state.GetPartitionIndexIfSinglePartition(UseFixedSizeMap());
	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = state.partition_pin_states[partition_index.GetIndex()];

		const auto size_before = partition.data_size;
		partition.AppendUnified(partition_pin_state, state.chunk_state, input, append_sel, actual_append_count);
		data_size += partition.data_size - size_before;
	} else {
		// Compute the heap sizes for the whole chunk
		if (!layout.AllConstant()) {
			TupleDataCollection::ComputeHeapSizes(state.chunk_state, input, state.partition_sel, actual_append_count);
		}

		// Build the buffer space
		BuildBufferSpace(state);

		// Now scatter everything in one go
		partitions[0]->Scatter(state.chunk_state, input, state.partition_sel, actual_append_count);
	}

	count += actual_append_count;
	Verify();
}

void PartitionedTupleData::Append(PartitionedTupleDataAppendState &state, TupleDataChunkState &input,
                                  const idx_t append_count) {
	const auto max_partition_idx = MaxPartitionIndex();
	const auto use_fixed_size_map = max_partition_idx < PartitionedTupleDataAppendState::MAP_THRESHOLD;

	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(input.row_locations, append_count, state.partition_indices, state.utility_vector);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, *FlatVector::IncrementalSelectionVector(), append_count);

	// Early out: check if everything belongs to a single partition
	auto partition_index = state.GetPartitionIndexIfSinglePartition(use_fixed_size_map);
	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = state.partition_pin_states[partition_index.GetIndex()];

		state.chunk_state.heap_sizes.Reference(input.heap_sizes);

		const auto size_before = partition.data_size;
		partition.Build(partition_pin_state, state.chunk_state, 0, append_count);
		data_size += partition.data_size - size_before;

		partition.CopyRows(state.chunk_state, input, *FlatVector::IncrementalSelectionVector(), append_count);
	} else {
		// Build the buffer space
		state.chunk_state.heap_sizes.Slice(input.heap_sizes, state.partition_sel, append_count);
		state.chunk_state.heap_sizes.Flatten();
		BuildBufferSpace(state);

		// Copy the rows
		partitions[0]->CopyRows(state.chunk_state, input, state.partition_sel, append_count);
	}

	count += append_count;
	Verify();
}

void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, const SelectionVector &append_sel,
                                             const idx_t append_count) const {
	if (UseFixedSizeMap()) {
		BuildPartitionSel<true>(state, append_sel, append_count, MaxPartitionIndex());
	} else {
		BuildPartitionSel<false>(state, append_sel, append_count, MaxPartitionIndex());
	}
}

template <bool FIXED>
static void AddPartitionEntry(PartitionedTupleDataAppendState &state, const idx_t partition_index) {
	using GETTER = TemplatedMapGetter<list_entry_t, FIXED>;
	auto &partition_entries = state.GetMap<FIXED>();
	auto partition_entry = partition_entries.emplace(partition_index, list_entry_t(0, 1));
	if (!partition_entry.second) {
		GETTER::GetValue(partition_entry.first).length++;
	}
}

template <>
void AddPartitionEntry<true>(PartitionedTupleDataAppendState &state, const idx_t partition_index) {
	bool inserted;
	auto &partition_entry = state.fixed_partition_entries.GetOrInsert(partition_index, inserted);
	if (inserted) {
		partition_entry = list_entry_t(0, 1);
	} else {
		partition_entry.length++;
	}
}

template <bool FIXED, bool HAS_SEL, bool COMPUTE_REVERSE_PARTITION_SEL>
static void TemplatedBuildPartitionSel(PartitionedTupleDataAppendState &state, const SelectionVector &append_sel,
                                       const idx_t append_count) {
	D_ASSERT(state.partition_indices.GetVectorType() == VectorType::FLAT_VECTOR);
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	auto &partition_sel = state.partition_sel;
	auto &reverse_partition_sel = state.reverse_partition_sel;
	auto &partition_offsets = state.partition_offsets;

	for (idx_t i = 0; i < append_count; i++) {
		const auto index = HAS_SEL ? append_sel[i] : i;
		const auto &partition_index = partition_indices[i];
		auto &partition_offset = partition_offsets[partition_index];
		if (COMPUTE_REVERSE_PARTITION_SEL) {
			reverse_partition_sel.set_index(index, partition_offset);
		}
		partition_sel[partition_offset++] = UnsafeNumericCast<sel_t>(index);
	}
}

template <bool FIXED>
void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, const SelectionVector &append_sel,
                                             const idx_t append_count, const idx_t max_partition_idx) {
	using GETTER = TemplatedMapGetter<list_entry_t, FIXED>;
	auto &partition_entries = state.GetMap<FIXED>();
	partition_entries.clear();

	if (max_partition_idx == 0 || state.partition_indices.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		idx_t partition_idx;
		if (state.partition_indices.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			partition_idx = *ConstantVector::GetData<idx_t>(state.partition_indices);
		} else {
			partition_idx = FlatVector::GetData<idx_t>(state.partition_indices)[0];
		}
		partition_entries[partition_idx] = list_entry_t(0, append_count);
	} else {
		D_ASSERT(state.partition_indices.GetVectorType() == VectorType::FLAT_VECTOR);
		const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
		for (idx_t i = 0; i < append_count; i++) {
			AddPartitionEntry<FIXED>(state, partition_indices[i]);
		}
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		if (!state.compute_reverse_partition_sel) {
			return;
		}
		// This needs to be initialized, even if we go the short path here
		if (append_sel.IsSet()) {
			for (sel_t i = 0; i < append_count; i++) {
				const auto index = append_sel.get_index(i);
				state.reverse_partition_sel[index] = i;
			}
		} else {
			for (sel_t i = 0; i < append_count; i++) {
				state.reverse_partition_sel[i] = i;
			}
		}
		return;
	}

	// Compute offsets from the counts
	if (state.partition_offsets.size() <= max_partition_idx) {
		state.partition_offsets.resize(max_partition_idx + 1);
	}
	idx_t offset = 0;
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		const auto &partition_index = GETTER::GetKey(it);
		auto &partition_entry = GETTER::GetValue(it);
		partition_entry.offset = offset;
		state.partition_offsets[partition_index] = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	if (append_sel.IsSet()) {
		if (state.compute_reverse_partition_sel) {
			TemplatedBuildPartitionSel<FIXED, true, true>(state, append_sel, append_count);
		} else {
			TemplatedBuildPartitionSel<FIXED, true, false>(state, append_sel, append_count);
		}
	} else {
		if (state.compute_reverse_partition_sel) {
			TemplatedBuildPartitionSel<FIXED, false, true>(state, append_sel, append_count);
		} else {
			TemplatedBuildPartitionSel<FIXED, false, false>(state, append_sel, append_count);
		}
	}
}

void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state) {
	if (UseFixedSizeMap()) {
		BuildBufferSpace<true>(state);
	} else {
		BuildBufferSpace<false>(state);
	}
}

template <bool FIXED>
void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state) {
	using GETTER = TemplatedMapGetter<list_entry_t, FIXED>;
	const auto &partition_entries = state.GetMap<FIXED>();
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		const auto &partition_index = GETTER::GetKey(it);

		// Partition, pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = GETTER::GetValue(it);
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset;

		// Build out the buffer space for this partition
		const auto size_before = partition.data_size;
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
		data_size += partition.data_size - size_before;
	}
}

void PartitionedTupleData::FlushAppendState(PartitionedTupleDataAppendState &state) {
	for (idx_t partition_index = 0; partition_index < partitions.size(); partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = state.partition_pin_states[partition_index];
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
	this->count += other.count;
	this->data_size += other.data_size;
	Verify();
}

void PartitionedTupleData::Reset() {
	for (auto &partition : partitions) {
		partition->Reset();
	}
	this->count = 0;
	this->data_size = 0;
	Verify();
}

void PartitionedTupleData::Repartition(ClientContext &context, PartitionedTupleData &new_partitioned_data) {
	D_ASSERT(layout.GetTypes() == new_partitioned_data.layout.GetTypes());

	if (partitions.size() == new_partitioned_data.partitions.size()) {
		new_partitioned_data.Combine(*this);
		return;
	}

	PartitionedTupleDataAppendState append_state;
	new_partitioned_data.InitializeAppendState(append_state);

	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		auto &partition = *partitions[partition_idx];

		if (partition.Count() > 0) {
			TupleDataChunkIterator iterator(partition, TupleDataPinProperties::DESTROY_AFTER_DONE, true);
			auto &chunk_state = iterator.GetChunkState();
			do {
				// Check for interrupts with each chunk
				context.InterruptCheck();
				new_partitioned_data.Append(append_state, chunk_state, iterator.GetCurrentChunkCount());
			} while (iterator.Next());

			RepartitionFinalizeStates(*this, new_partitioned_data, append_state, partition_idx);
		}
		partitions[partition_idx]->Reset();
	}
	new_partitioned_data.FlushAppendState(append_state);

	count = 0;
	data_size = 0;

	Verify();
}

void PartitionedTupleData::Unpin() {
	for (auto &partition : partitions) {
		partition->Unpin();
	}
}

unsafe_vector<unique_ptr<TupleDataCollection>> &PartitionedTupleData::GetPartitions() {
	return partitions;
}

unique_ptr<TupleDataCollection> PartitionedTupleData::GetUnpartitioned() {
	auto data_collection = std::move(partitions[0]);
	partitions[0] = make_uniq<TupleDataCollection>(buffer_manager, layout_ptr, tag);

	for (idx_t i = 1; i < partitions.size(); i++) {
		data_collection->Combine(*partitions[i]);
	}
	count = 0;
	data_size = 0;

	data_collection->Verify();
	Verify();

	return data_collection;
}

idx_t PartitionedTupleData::Count() const {
	return count;
}

idx_t PartitionedTupleData::SizeInBytes() const {
	return data_size + stl_allocator->AllocationSize();
}

idx_t PartitionedTupleData::PartitionCount() const {
	return partitions.size();
}

void PartitionedTupleData::GetSizesAndCounts(vector<idx_t> &partition_sizes, vector<idx_t> &partition_counts) const {
	D_ASSERT(partition_sizes.size() == PartitionCount());
	D_ASSERT(partition_sizes.size() == partition_counts.size());
	for (idx_t i = 0; i < PartitionCount(); i++) {
		auto &partition = *partitions[i];
		partition_sizes[i] += partition.SizeInBytes();
		partition_counts[i] += partition.Count();
	}
}

void PartitionedTupleData::Verify() const {
#ifdef D_ASSERT_IS_ENABLED
	idx_t total_count = 0;
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		partition->Verify();
		total_count += partition->Count();
		total_size += partition->data_size;
	}
	D_ASSERT(total_count == this->count);
	D_ASSERT(total_size == this->data_size);
#endif
}

// LCOV_EXCL_START
string PartitionedTupleData::ToString() {
	string result =
	    StringUtil::Format("PartitionedTupleData - [%llu Partitions, %llu Rows]\n", partitions.size(), Count());
	for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
		result += StringUtil::Format("Partition %llu: ", partition_idx) + partitions[partition_idx]->ToString();
	}
	return result;
}

void PartitionedTupleData::Print() {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

} // namespace duckdb
