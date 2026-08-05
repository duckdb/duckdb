#include "duckdb/execution/aggregate_state_spilling.hpp"

#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

// The deserialization of primitive string fields stores string_t values that point into the
// input vector. Sort keys and lists already copy into the allocator; this pass copies the rest,
// so that the imported states do not dangle once the exported data is destroyed.
static void MaterializeStateStrings(const LogicalType &type, const AggregateStateField &field, data_ptr_t state,
                                    idx_t base, ArenaAllocator &allocator) {
	switch (field.kind) {
	case AggregateFieldKind::OPTIONAL_VALUE:
		D_ASSERT(field.children.size() == 1);
		if (!Load<bool>(state + base + field.field_offset)) {
			return;
		}
		MaterializeStateStrings(type, field.children[0], state, base, allocator);
		break;
	case AggregateFieldKind::STRUCT: {
		const auto &child_types = StructType::GetChildTypes(type);
		const idx_t new_base = base + field.field_offset;
		for (idx_t child_idx = 0; child_idx < field.children.size(); child_idx++) {
			MaterializeStateStrings(child_types[child_idx].second, field.children[child_idx], state, new_base,
			                        allocator);
		}
		break;
	}
	case AggregateFieldKind::PRIMITIVE:
		if (type.InternalType() == PhysicalType::VARCHAR) {
			const auto value = Load<string_t>(state + base + field.field_offset);
			if (!value.IsInlined()) {
				const auto size = value.GetSize();
				auto buf = char_ptr_cast(allocator.Allocate(size));
				memcpy(buf, value.GetData(), size);
				Store<string_t>(string_t(buf, UnsafeNumericCast<uint32_t>(size)), state + base + field.field_offset);
			}
		}
		break;
	default:
		break;
	}
}

// Whether this field can hold a reference into the aggregate arena. Pointer-free states
// (counts, sums, inline values) never need exporting, their rows spill on their own.
static bool FieldHasArenaReferences(const LogicalType &type, const AggregateStateField &field) {
	switch (field.kind) {
	case AggregateFieldKind::OPTIONAL_VALUE:
		D_ASSERT(field.children.size() == 1);
		return FieldHasArenaReferences(type, field.children[0]);
	case AggregateFieldKind::STRUCT: {
		const auto &child_types = StructType::GetChildTypes(type);
		for (idx_t child_idx = 0; child_idx < field.children.size(); child_idx++) {
			if (FieldHasArenaReferences(child_types[child_idx].second, field.children[child_idx])) {
				return true;
			}
		}
		return false;
	}
	case AggregateFieldKind::PRIMITIVE:
		return type.InternalType() == PhysicalType::VARCHAR;
	case AggregateFieldKind::SORT_KEY:
	case AggregateFieldKind::LIST:
		return true;
	default:
		return true;
	}
}

bool AggregateStateSpilling::CanSpill(const TupleDataLayout &layout) {
	auto &aggregates = layout.GetAggregates();
	if (aggregates.empty()) {
		return false;
	}
	for (auto &aggr : aggregates) {
		if (!aggr.function.HasGetStateTypeCallback()) {
			// the aggregate cannot describe its state
			return false;
		}
		if (aggr.function.HasStateDestructorCallback()) {
			// the state owns resources beyond the arena
			return false;
		}
	}
	// only layouts whose states can reference the arena benefit from exporting
	for (auto &state_layout : StateLayouts(layout)) {
		if (FieldHasArenaReferences(state_layout.type, state_layout.field)) {
			return true;
		}
	}
	return false;
}

vector<AggregateStateLayout> AggregateStateSpilling::StateLayouts(const TupleDataLayout &layout) {
	vector<AggregateStateLayout> result;
	result.reserve(layout.GetAggregates().size());
	for (auto &aggr : layout.GetAggregates()) {
		result.push_back(aggr.function.GetStateType(aggr.GetFunctionData()));
	}
	return result;
}

vector<LogicalType> AggregateStateSpilling::ExportedTypes(const TupleDataLayout &layout) {
	auto types = layout.GetTypes();
	for (auto &state_layout : StateLayouts(layout)) {
		types.push_back(state_layout.type);
	}
	return types;
}

void AggregateStateSpilling::ExportStates(ClientContext &context, const TupleDataLayout &layout,
                                          const vector<AggregateStateLayout> &state_layouts,
                                          TupleDataCollection &source,
                                          vector<unique_ptr<ColumnDataCollection>> &exported, idx_t exported_radix_bits,
                                          const vector<LogicalType> &exported_types, ArenaAllocator &allocator) {
	if (source.Count() == 0) {
		return;
	}
	auto &aggregates = layout.GetAggregates();
	const auto column_count = layout.ColumnCount();

	DataChunk group_chunk;
	group_chunk.Initialize(Allocator::Get(context), layout.GetTypes());
	DataChunk exported_chunk;
	exported_chunk.Initialize(Allocator::Get(context), ExportedTypes(layout));

	Vector state_addresses(LogicalType::POINTER);
	auto state_address_data = FlatVector::GetDataMutable<data_ptr_t>(state_addresses);

	const auto hash_col_idx = layout.ColumnCount() - 1;
	DataChunk target_chunk;
	target_chunk.Initialize(Allocator::Get(context), exported_types);
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	TupleDataScanState scan_state;
	source.InitializeScan(scan_state, TupleDataPinProperties::DESTROY_AFTER_DONE);
	while (source.Scan(scan_state, group_chunk)) {
		const auto count = group_chunk.size();
		exported_chunk.Reset();
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			exported_chunk.data[col_idx].Reference(group_chunk.data[col_idx]);
		}

		const auto row_locations = FlatVector::GetData<data_ptr_t>(scan_state.chunk_state.row_locations);
		idx_t aggr_offset = layout.GetAggrOffset();
		for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			auto &aggr = aggregates[aggr_idx];
			for (idx_t i = 0; i < count; i++) {
				state_address_data[i] = row_locations[i] + aggr_offset;
			}
			auto &result = exported_chunk.data[column_count + aggr_idx];
			AggregateStateSerialization::SerializeStates(aggr.function, aggr.GetFunctionData(), state_layouts[aggr_idx],
			                                             state_addresses, count, result, allocator, 0);
			aggr_offset += aggr.payload_size;
		}
		exported_chunk.SetChildCardinality(count);

		// route the rows to their exported partitions. rows of one source partition can only go
		// to the partitions covering the same hash prefix, so the number of targets is small
		const auto hashes = FlatVector::GetData<hash_t>(group_chunk.data[hash_col_idx]);
		idx_t partition_of[STANDARD_VECTOR_SIZE];
		for (idx_t i = 0; i < count; i++) {
			partition_of[i] = RadixPartitioning::ApplyMask(hashes[i], exported_radix_bits);
		}
		idx_t routed = 0;
		while (routed < count) {
			// find the first row that has not been routed yet, and route all rows of its partition
			idx_t partition_idx = DConstants::INVALID_INDEX;
			idx_t sel_count = 0;
			for (idx_t i = 0; i < count; i++) {
				if (partition_of[i] == DConstants::INVALID_INDEX) {
					continue;
				}
				if (partition_idx == DConstants::INVALID_INDEX) {
					partition_idx = partition_of[i];
				}
				if (partition_of[i] == partition_idx) {
					sel.set_index(sel_count++, i);
					partition_of[i] = DConstants::INVALID_INDEX;
				}
			}
			routed += sel_count;
			auto &target = exported[partition_idx];
			if (!target) {
				target = make_uniq<ColumnDataCollection>(BufferManager::GetBufferManager(context), exported_types);
			}
			if (sel_count == count) {
				target->Append(exported_chunk);
				break;
			}
			target_chunk.Reset();
			target_chunk.Slice(exported_chunk, sel, sel_count);
			target->Append(target_chunk);
		}
	}
}

void AggregateStateSpilling::ImportStates(ClientContext &context, shared_ptr<TupleDataLayout> layout,
                                          const vector<AggregateStateLayout> &state_layouts,
                                          ColumnDataCollection &exported, ArenaAllocator &allocator,
                                          const std::function<void(TupleDataCollection &)> &combine) {
	if (exported.Count() == 0) {
		return;
	}
	auto &aggregates = layout->GetAggregates();
	const auto column_count = layout->ColumnCount();

	DataChunk exported_chunk;
	exported_chunk.Initialize(Allocator::Get(context), exported.Types());
	DataChunk group_chunk;
	group_chunk.Initialize(Allocator::Get(context), layout->GetTypes());

	// scratch buffer holding one vector of deserialized states, packed consecutively
	idx_t max_state_size = 0;
	for (auto &state_layout : state_layouts) {
		max_state_size = MaxValue(max_state_size, state_layout.total_state_size);
	}
	auto state_buffer = make_unsafe_uniq_array<data_t>(max_state_size * STANDARD_VECTOR_SIZE);
	Vector state_addresses(LogicalType::POINTER);
	auto state_address_data = FlatVector::GetDataMutable<data_ptr_t>(state_addresses);

	ColumnDataScanState scan_state;
	exported.InitializeScan(scan_state, ColumnDataScanProperties::DISALLOW_ZERO_COPY);
	while (exported.Scan(scan_state, exported_chunk)) {
		const auto count = exported_chunk.size();
		auto result = make_uniq<TupleDataCollection>(BufferManager::GetBufferManager(context), layout,
		                                             MemoryTag::HASH_TABLE, nullptr, context);
		TupleDataAppendState append_state;
		result->InitializeAppend(append_state, TupleDataPinProperties::UNPIN_AFTER_DONE);
		group_chunk.Reset();
		for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
			group_chunk.data[col_idx].Reference(exported_chunk.data[col_idx]);
		}
		group_chunk.SetChildCardinality(count);
		result->Append(append_state, group_chunk);

		const auto row_locations = FlatVector::GetData<data_ptr_t>(append_state.chunk_state.row_locations);
		idx_t aggr_offset = layout->GetAggrOffset();
		for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
			auto &aggr = aggregates[aggr_idx];
			auto &state_layout = state_layouts[aggr_idx];
			const auto state_size = state_layout.total_state_size;
			D_ASSERT(state_size == aggr.payload_size);

			// initialize scratch states, deserialize into them, then place them in the rows
			AggregateStateInput state_input(aggr.function, aggr.GetFunctionData());
			for (idx_t i = 0; i < count; i++) {
				state_address_data[i] = state_buffer.get() + i * state_size;
				aggr.function.GetStateInitCallback()(state_input, &state_address_data[i], 1);
			}
			AggregateStateSerialization::DeserializeStates(aggr.function, state_layout,
			                                               exported_chunk.data[column_count + aggr_idx], count,
			                                               state_buffer.get(), allocator);
			for (idx_t i = 0; i < count; i++) {
				MaterializeStateStrings(state_layout.type, state_layout.field, state_buffer.get() + i * state_size, 0,
				                        allocator);
			}
			for (idx_t i = 0; i < count; i++) {
				memcpy(row_locations[i] + aggr_offset, state_buffer.get() + i * state_size, state_size);
			}
			aggr_offset += aggr.payload_size;
		}
		combine(*result);
	}
	exported.Reset();
}

} // namespace duckdb
