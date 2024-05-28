#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PartitionedTupleData::PartitionedTupleData(PartitionedTupleDataType type_p, BufferManager &buffer_manager_p,
                                           const TupleDataLayout &layout_p)
    : type(type_p), buffer_manager(buffer_manager_p), layout(layout_p.Copy()), count(0), data_size(0),
      allocators(make_shared_ptr<PartitionTupleDataAllocators>()) {
}

PartitionedTupleData::PartitionedTupleData(const PartitionedTupleData &other)
    : type(other.type), buffer_manager(other.buffer_manager), layout(other.layout.Copy()) {
}

PartitionedTupleData::~PartitionedTupleData() {
}

const TupleDataLayout &PartitionedTupleData::GetLayout() const {
	return layout;
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
	ComputePartitionIndices(state, input);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, append_sel, actual_append_count);

	// Early out: check if everything belongs to a single partition
	optional_idx partition_index;
	if (UseFixedSizeMap()) {
		if (state.fixed_partition_entries.size() == 1) {
			partition_index = state.fixed_partition_entries.begin().GetKey();
		}
	} else {
		if (state.partition_entries.size() == 1) {
			partition_index = state.partition_entries.begin()->first;
		}
	}
	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = *state.partition_pin_states[partition_index.GetIndex()];

		const auto size_before = partition.SizeInBytes();
		partition.AppendUnified(partition_pin_state, state.chunk_state, input, append_sel, actual_append_count);
		data_size += partition.SizeInBytes() - size_before;
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
	// Compute partition indices and store them in state.partition_indices
	ComputePartitionIndices(input.row_locations, append_count, state.partition_indices);

	// Build the selection vector for the partitions
	BuildPartitionSel(state, *FlatVector::IncrementalSelectionVector(), append_count);

	// Early out: check if everything belongs to a single partition
	optional_idx partition_index;
	if (UseFixedSizeMap()) {
		if (state.fixed_partition_entries.size() == 1) {
			partition_index = state.fixed_partition_entries.begin().GetKey();
		}
	} else {
		if (state.partition_entries.size() == 1) {
			partition_index = state.partition_entries.begin()->first;
		}
	}

	if (partition_index.IsValid()) {
		auto &partition = *partitions[partition_index.GetIndex()];
		auto &partition_pin_state = *state.partition_pin_states[partition_index.GetIndex()];

		state.chunk_state.heap_sizes.Reference(input.heap_sizes);

		const auto size_before = partition.SizeInBytes();
		partition.Build(partition_pin_state, state.chunk_state, 0, append_count);
		data_size += partition.SizeInBytes() - size_before;

		partition.CopyRows(state.chunk_state, input, *FlatVector::IncrementalSelectionVector(), append_count);
	} else {
		// Build the buffer space
		state.chunk_state.heap_sizes.Slice(input.heap_sizes, state.partition_sel, append_count);
		state.chunk_state.heap_sizes.Flatten(append_count);
		BuildBufferSpace(state);

		// Copy the rows
		partitions[0]->CopyRows(state.chunk_state, input, state.partition_sel, append_count);
	}

	count += append_count;
	Verify();
}

// LCOV_EXCL_START
template <class MAP_TYPE>
struct UnorderedMapGetter {
	static inline const typename MAP_TYPE::key_type &GetKey(typename MAP_TYPE::iterator &iterator) {
		return iterator->first;
	}

	static inline const typename MAP_TYPE::key_type &GetKey(const typename MAP_TYPE::const_iterator &iterator) {
		return iterator->first;
	}

	static inline typename MAP_TYPE::mapped_type &GetValue(typename MAP_TYPE::iterator &iterator) {
		return iterator->second;
	}

	static inline const typename MAP_TYPE::mapped_type &GetValue(const typename MAP_TYPE::const_iterator &iterator) {
		return iterator->second;
	}
};

template <class T>
struct FixedSizeMapGetter {
	static inline const idx_t &GetKey(fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetKey();
	}

	static inline const idx_t &GetKey(const const_fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetKey();
	}

	static inline T &GetValue(fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetValue();
	}

	static inline const T &GetValue(const const_fixed_size_map_iterator_t<T> &iterator) {
		return iterator.GetValue();
	}
};
// LCOV_EXCL_STOP

void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, const SelectionVector &append_sel,
                                             const idx_t append_count) {
	if (UseFixedSizeMap()) {
		BuildPartitionSel<fixed_size_map_t<list_entry_t>, FixedSizeMapGetter<list_entry_t>>(
		    state, state.fixed_partition_entries, append_sel, append_count);
	} else {
		BuildPartitionSel<perfect_map_t<list_entry_t>, UnorderedMapGetter<perfect_map_t<list_entry_t>>>(
		    state, state.partition_entries, append_sel, append_count);
	}
}

template <class MAP_TYPE, class GETTER>
void PartitionedTupleData::BuildPartitionSel(PartitionedTupleDataAppendState &state, MAP_TYPE &partition_entries,
                                             const SelectionVector &append_sel, const idx_t append_count) {
	const auto partition_indices = FlatVector::GetData<idx_t>(state.partition_indices);
	partition_entries.clear();

	switch (state.partition_indices.GetVectorType()) {
	case VectorType::FLAT_VECTOR:
		for (idx_t i = 0; i < append_count; i++) {
			const auto index = append_sel.get_index(i);
			const auto &partition_index = partition_indices[index];
			auto partition_entry = partition_entries.find(partition_index);
			if (partition_entry == partition_entries.end()) {
				partition_entries[partition_index] = list_entry_t(0, 1);
			} else {
				GETTER::GetValue(partition_entry).length++;
			}
		}
		break;
	case VectorType::CONSTANT_VECTOR:
		partition_entries[partition_indices[0]] = list_entry_t(0, append_count);
		break;
	default:
		throw InternalException("Unexpected VectorType in PartitionedTupleData::Append");
	}

	// Early out: check if everything belongs to a single partition
	if (partition_entries.size() == 1) {
		// This needs to be initialized, even if we go the short path here
		for (idx_t i = 0; i < append_count; i++) {
			const auto index = append_sel.get_index(i);
			state.reverse_partition_sel[index] = NumericCast<sel_t>(i);
		}
		return;
	}

	// Compute offsets from the counts
	idx_t offset = 0;
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		auto &partition_entry = GETTER::GetValue(it);
		partition_entry.offset = offset;
		offset += partition_entry.length;
	}

	// Now initialize a single selection vector that acts as a selection vector for every partition
	auto &partition_sel = state.partition_sel;
	auto &reverse_partition_sel = state.reverse_partition_sel;
	for (idx_t i = 0; i < append_count; i++) {
		const auto index = append_sel.get_index(i);
		const auto &partition_index = partition_indices[index];
		auto &partition_offset = partition_entries[partition_index].offset;
		reverse_partition_sel[index] = UnsafeNumericCast<sel_t>(partition_offset);
		partition_sel[partition_offset++] = UnsafeNumericCast<sel_t>(index);
	}
}

void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state) {
	if (UseFixedSizeMap()) {
		BuildBufferSpace<fixed_size_map_t<list_entry_t>, FixedSizeMapGetter<list_entry_t>>(
		    state, state.fixed_partition_entries);
	} else {
		BuildBufferSpace<perfect_map_t<list_entry_t>, UnorderedMapGetter<perfect_map_t<list_entry_t>>>(
		    state, state.partition_entries);
	}
}

template <class MAP_TYPE, class GETTER>
void PartitionedTupleData::BuildBufferSpace(PartitionedTupleDataAppendState &state, const MAP_TYPE &partition_entries) {
	for (auto it = partition_entries.begin(); it != partition_entries.end(); ++it) {
		const auto &partition_index = GETTER::GetKey(it);

		// Partition, pin state for this partition index
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];

		// Length and offset for this partition
		const auto &partition_entry = GETTER::GetValue(it);
		const auto &partition_length = partition_entry.length;
		const auto partition_offset = partition_entry.offset - partition_length;

		// Build out the buffer space for this partition
		const auto size_before = partition.SizeInBytes();
		partition.Build(partition_pin_state, state.chunk_state, partition_offset, partition_length);
		data_size += partition.SizeInBytes() - size_before;
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

void PartitionedTupleData::Repartition(PartitionedTupleData &new_partitioned_data) {
	D_ASSERT(layout.GetTypes() == new_partitioned_data.layout.GetTypes());

	if (partitions.size() == new_partitioned_data.partitions.size()) {
		new_partitioned_data.Combine(*this);
		return;
	}

	PartitionedTupleDataAppendState append_state;
	new_partitioned_data.InitializeAppendState(append_state);

	const auto reverse = RepartitionReverseOrder();
	const idx_t start_idx = reverse ? partitions.size() : 0;
	const idx_t end_idx = reverse ? 0 : partitions.size();
	const int64_t update = reverse ? -1 : 1;
	const int64_t adjustment = reverse ? -1 : 0;

	for (idx_t partition_idx = start_idx; partition_idx != end_idx; partition_idx += idx_t(update)) {
		auto actual_partition_idx = partition_idx + idx_t(adjustment);
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
	partitions[0] = make_uniq<TupleDataCollection>(buffer_manager, layout);

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
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		total_size += partition->SizeInBytes();
	}
	return total_size;
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
#ifdef DEBUG
	idx_t total_count = 0;
	idx_t total_size = 0;
	for (auto &partition : partitions) {
		partition->Verify();
		total_count += partition->Count();
		total_size += partition->SizeInBytes();
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

void PartitionedTupleData::CreateAllocator() {
	allocators->allocators.emplace_back(make_shared_ptr<TupleDataAllocator>(buffer_manager, layout));
}

} // namespace duckdb
