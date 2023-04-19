#include "duckdb/common/radix_partitioning.hpp"

#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

namespace duckdb {

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE RadixBitsSwitch(idx_t radix_bits, ARGS &&...args) {
	D_ASSERT(radix_bits <= sizeof(hash_t) * 8);
	switch (radix_bits) {
	case 1:
		return OP::template Operation<1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<4>(std::forward<ARGS>(args)...);
	case 5:
		return OP::template Operation<5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

template <idx_t radix_bits>
struct RadixLessThan {
	static inline bool Operation(hash_t hash, hash_t cutoff) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;
		return CONSTANTS::ApplyMask(hash) < cutoff;
	}
};

struct SelectFunctor {
	template <idx_t radix_bits>
	static idx_t Operation(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t cutoff,
	                       SelectionVector *true_sel, SelectionVector *false_sel) {
		Vector cutoff_vector(Value::HASH(cutoff));
		return BinaryExecutor::Select<hash_t, hash_t, RadixLessThan<radix_bits>>(hashes, cutoff_vector, sel, count,
		                                                                         true_sel, false_sel);
	}
};

idx_t RadixPartitioning::Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return RadixBitsSwitch<SelectFunctor, idx_t>(radix_bits, hashes, sel, count, cutoff, true_sel, false_sel);
}

struct ComputePartitionIndicesFunctor {
	template <idx_t radix_bits>
	static void Operation(Vector &hashes, Vector &partition_indices, idx_t count) {
		UnaryExecutor::Execute<hash_t, hash_t>(hashes, partition_indices, count, [&](hash_t hash) {
			using CONSTANTS = RadixPartitioningConstants<radix_bits>;
			return CONSTANTS::ApplyMask(hash);
		});
	}
};

//===--------------------------------------------------------------------===//
// Column Data Partitioning
//===--------------------------------------------------------------------===//
RadixPartitionedColumnData::RadixPartitionedColumnData(ClientContext &context_p, vector<LogicalType> types_p,
                                                       idx_t radix_bits_p, idx_t hash_col_idx_p)
    : PartitionedColumnData(PartitionedColumnDataType::RADIX, context_p, std::move(types_p)), radix_bits(radix_bits_p),
      hash_col_idx(hash_col_idx_p) {
	D_ASSERT(hash_col_idx < types.size());
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	allocators->allocators.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		CreateAllocator();
	}
	D_ASSERT(allocators->allocators.size() == num_partitions);
}

RadixPartitionedColumnData::RadixPartitionedColumnData(const RadixPartitionedColumnData &other)
    : PartitionedColumnData(other), radix_bits(other.radix_bits), hash_col_idx(other.hash_col_idx) {
	for (idx_t i = 0; i < RadixPartitioning::NumberOfPartitions(radix_bits); i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
	}
}

RadixPartitionedColumnData::~RadixPartitionedColumnData() {
}

void RadixPartitionedColumnData::InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	state.partition_append_states.reserve(num_partitions);
	state.partition_buffers.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		state.partition_append_states.emplace_back(make_uniq<ColumnDataAppendState>());
		partitions[i]->InitializeAppend(*state.partition_append_states[i]);
		state.partition_buffers.emplace_back(CreatePartitionBuffer());
	}
}

void RadixPartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
	D_ASSERT(partitions.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	D_ASSERT(state.partition_buffers.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, input.data[hash_col_idx], state.partition_indices,
	                                                      input.size());
}

//===--------------------------------------------------------------------===//
// Tuple Data Partitioning
//===--------------------------------------------------------------------===//
RadixPartitionedTupleData::RadixPartitionedTupleData(BufferManager &buffer_manager, const TupleDataLayout &layout_p,
                                                     idx_t radix_bits_p, idx_t hash_col_idx_p)
    : PartitionedTupleData(PartitionedTupleDataType::RADIX, buffer_manager, layout_p.Copy()), radix_bits(radix_bits_p),
      hash_col_idx(hash_col_idx_p) {
	D_ASSERT(hash_col_idx < layout.GetTypes().size());
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	allocators->allocators.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		CreateAllocator();
	}
	D_ASSERT(allocators->allocators.size() == num_partitions);
	Initialize();
}

RadixPartitionedTupleData::RadixPartitionedTupleData(const RadixPartitionedTupleData &other)
    : PartitionedTupleData(other), radix_bits(other.radix_bits), hash_col_idx(other.hash_col_idx) {
	Initialize();
}

RadixPartitionedTupleData::~RadixPartitionedTupleData() {
}

void RadixPartitionedTupleData::Initialize() {
	for (idx_t i = 0; i < RadixPartitioning::NumberOfPartitions(radix_bits); i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
	}
}

void RadixPartitionedTupleData::InitializeAppendStateInternal(PartitionedTupleDataAppendState &state,
                                                              TupleDataPinProperties properties) const {
	// Init pin state per partition
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	state.partition_pin_states.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		state.partition_pin_states.emplace_back(make_uniq<TupleDataPinState>());
		partitions[i]->InitializeAppend(*state.partition_pin_states[i], properties);
	}

	// Init single chunk state
	auto column_count = layout.ColumnCount();
	vector<column_t> column_ids;
	column_ids.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		column_ids.emplace_back(col_idx);
	}
	partitions[0]->InitializeAppend(state.chunk_state, std::move(column_ids));
}

void RadixPartitionedTupleData::ComputePartitionIndices(PartitionedTupleDataAppendState &state, DataChunk &input) {
	D_ASSERT(partitions.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, input.data[hash_col_idx], state.partition_indices,
	                                                      input.size());
}

void RadixPartitionedTupleData::ComputePartitionIndices(Vector &row_locations, idx_t count,
                                                        Vector &partition_indices) const {
	Vector intermediate(LogicalType::HASH);
	partitions[0]->Gather(row_locations, *FlatVector::IncrementalSelectionVector(), count, hash_col_idx, intermediate,
	                      *FlatVector::IncrementalSelectionVector());
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, intermediate, partition_indices, count);
}

void RadixPartitionedTupleData::RepartitionFinalizeStates(PartitionedTupleData &old_partitioned_data,
                                                          PartitionedTupleData &new_partitioned_data,
                                                          PartitionedTupleDataAppendState &state,
                                                          idx_t finished_partition_idx) const {
	D_ASSERT(old_partitioned_data.GetType() == PartitionedTupleDataType::RADIX &&
	         new_partitioned_data.GetType() == PartitionedTupleDataType::RADIX);
	const auto &old_radix_partitions = (RadixPartitionedTupleData &)old_partitioned_data;
	const auto &new_radix_partitions = (RadixPartitionedTupleData &)new_partitioned_data;
	const auto old_radix_bits = old_radix_partitions.GetRadixBits();
	const auto new_radix_bits = new_radix_partitions.GetRadixBits();
	D_ASSERT(new_radix_bits > old_radix_bits);

	// We take the most significant digits as the partition index
	// When repartitioning, e.g., partition 0 from "old" goes into the first N partitions in "new"
	// When partition 0 is done, we can already finalize the append states, unpinning blocks
	const auto multiplier = RadixPartitioning::NumberOfPartitions(new_radix_bits - old_radix_bits);
	const auto from_idx = finished_partition_idx * multiplier;
	const auto to_idx = from_idx + multiplier;
	auto &partitions = new_partitioned_data.GetPartitions();
	for (idx_t partition_index = from_idx; partition_index < to_idx; partition_index++) {
		auto &partition = *partitions[partition_index];
		auto &partition_pin_state = *state.partition_pin_states[partition_index];
		partition.FinalizePinState(partition_pin_state);
	}
}

} // namespace duckdb
