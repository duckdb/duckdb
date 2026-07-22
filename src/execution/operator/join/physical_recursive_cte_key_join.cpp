#include "duckdb/execution/operator/join/physical_recursive_cte_key_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

RecursiveCTEKeyJoinLayout::RecursiveCTEKeyJoinLayout(PhysicalRecursiveCTEStateScan &state_scan_p,
                                                     PhysicalOperator &probe, bool state_on_left_p,
                                                     vector<idx_t> state_key_indices_p,
                                                     vector<idx_t> probe_key_indices_p,
                                                     vector<idx_t> left_projection_map_p,
                                                     vector<idx_t> right_projection_map_p)
    : state_scan(state_scan_p), state_on_left(state_on_left_p), state_key_indices(std::move(state_key_indices_p)),
      probe_key_indices(std::move(probe_key_indices_p)), left_projection_map(std::move(left_projection_map_p)),
      right_projection_map(std::move(right_projection_map_p)),
      state_key_map(state_scan.GetTypes().size(), DConstants::INVALID_INDEX),
      state_payload_map(state_scan.GetTypes().size(), DConstants::INVALID_INDEX) {
	if (state_key_indices.empty() || state_key_indices.size() != probe_key_indices.size() ||
	    state_key_indices.size() > state_scan.distinct_idx.size() ||
	    !std::is_sorted(state_key_indices.begin(), state_key_indices.end()) ||
	    std::adjacent_find(state_key_indices.begin(), state_key_indices.end()) != state_key_indices.end()) {
		throw InternalException("Invalid USING KEY join key layout");
	}
	for (idx_t key_idx = 0; key_idx < state_scan.distinct_idx.size(); key_idx++) {
		const auto state_idx = state_scan.distinct_idx[key_idx];
		if (state_idx >= state_scan.GetTypes().size()) {
			throw InternalException("Invalid USING KEY state key ordinal");
		}
		state_key_map[state_idx] = key_idx;
		key_types.push_back(state_scan.GetTypes()[state_idx]);
	}
	for (idx_t join_key_idx = 0; join_key_idx < state_key_indices.size(); join_key_idx++) {
		const auto state_key_idx = state_key_indices[join_key_idx];
		const auto probe_key_idx = probe_key_indices[join_key_idx];
		if (state_key_idx >= key_types.size() || probe_key_idx >= probe.GetTypes().size() ||
		    key_types[state_key_idx] != probe.GetTypes()[probe_key_idx]) {
			throw InternalException("Invalid USING KEY join key ordinal");
		}
		probe_key_types.push_back(key_types[state_key_idx]);
	}
	for (idx_t payload_idx = 0; payload_idx < state_scan.payload_idx.size(); payload_idx++) {
		const auto state_idx = state_scan.payload_idx[payload_idx];
		if (state_idx >= state_scan.GetTypes().size()) {
			throw InternalException("Invalid USING KEY state payload ordinal");
		}
		state_payload_map[state_idx] = payload_idx;
		payload_types.push_back(state_scan.GetTypes()[state_idx]);
	}
	const auto &left_types = state_on_left ? state_scan.GetTypes() : probe.GetTypes();
	const auto &right_types = state_on_left ? probe.GetTypes() : state_scan.GetTypes();
	for (auto projection_idx : left_projection_map) {
		if (projection_idx >= left_types.size()) {
			throw InternalException("Invalid USING KEY left projection ordinal");
		}
	}
	for (auto projection_idx : right_projection_map) {
		if (projection_idx >= right_types.size()) {
			throw InternalException("Invalid USING KEY right projection ordinal");
		}
	}
}

bool RecursiveCTEKeyJoinLayout::IsPartial() const {
	return state_key_indices.size() < state_scan.distinct_idx.size();
}

PhysicalRecursiveCTEKeyJoin::PhysicalRecursiveCTEKeyJoin(
    PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &probe,
    PhysicalRecursiveCTEStateScan &state_scan_p, bool state_on_left_p, vector<idx_t> state_key_indices_p,
    vector<idx_t> probe_key_indices_p, vector<idx_t> left_projection_map_p, vector<idx_t> right_projection_map_p,
    idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_KEY_JOIN, op.types, estimated_cardinality),
      layout(state_scan_p, probe, state_on_left_p, std::move(state_key_indices_p), std::move(probe_key_indices_p),
             std::move(left_projection_map_p), std::move(right_projection_map_p)) {
	children.push_back(probe);
}

struct RecursiveCTEKeyJoinResult {
	idx_t match_count;
	OperatorResultType result_type;
};

class RecursiveCTEKeyJoinState : public CachingOperatorState {
public:
	RecursiveCTEKeyJoinState(ClientContext &context, const PhysicalRecursiveCTEKeyJoin &op)
	    : non_null_sel(STANDARD_VECTOR_SIZE), found_key_sel(STANDARD_VECTOR_SIZE),
	      matched_input_sel(STANDARD_VECTOR_SIZE), candidate_input_sel(STANDARD_VECTOR_SIZE),
	      candidate_match_sel(STANDARD_VECTOR_SIZE), candidate_addresses(LogicalType::POINTER),
	      matched_addresses(LogicalType::POINTER), probe_hashes(LogicalType::HASH),
	      key_formats(op.Layout().ProbeKeyTypes().size()), arena(Allocator::Get(context)), row_state(arena) {
		probe_keys.Initialize(Allocator::Get(context), op.Layout().ProbeKeyTypes());
		lookup_keys.Initialize(Allocator::Get(context), op.Layout().ProbeKeyTypes());
		candidate_keys.Initialize(Allocator::Get(context), op.Layout().KeyTypes());
		state_keys.Initialize(Allocator::Get(context), op.Layout().KeyTypes());
		payload_rows.Initialize(Allocator::Get(context), op.Layout().PayloadTypes());
	}

	OperatorResultType Execute(DataChunk &input, DataChunk &output, const RecursiveCTEKeyJoinLayout &layout,
	                           RecursiveCTEState &recursive_state);

	bool SupportsReuse() const override {
		return true;
	}

	void Reset() override {
		ResetCachingState();
		partial_input_initialized = false;
		active_probe = false;
	}

private:
	RecursiveCTEKeyJoinResult ProbeCompleteKey(DataChunk &input, const RecursiveCTEKeyJoinLayout &layout,
	                                           RecursiveCTEState &recursive_state);
	RecursiveCTEKeyJoinResult ProbePartialKey(DataChunk &input, const RecursiveCTEKeyJoinLayout &layout,
	                                          RecursiveCTEState &recursive_state);
	void FinalizePayload(const RecursiveCTEKeyJoinLayout &layout, RecursiveCTEState &recursive_state,
	                     idx_t match_count);
	void EmitResult(DataChunk &input, DataChunk &output, const RecursiveCTEKeyJoinLayout &layout, idx_t match_count);

private:
	DataChunk probe_keys;
	DataChunk lookup_keys;
	DataChunk candidate_keys;
	DataChunk state_keys;
	DataChunk payload_rows;
	SelectionVector non_null_sel;
	SelectionVector found_key_sel;
	SelectionVector matched_input_sel;
	SelectionVector candidate_input_sel;
	SelectionVector candidate_match_sel;
	Vector candidate_addresses;
	Vector matched_addresses;
	Vector probe_hashes;
	vector<UnifiedVectorFormat> key_formats;
	AggregateHTLookupState lookup_state;
	TupleDataChunkState match_chunk_state;
	RowMatcher partial_matcher;
	ArenaAllocator arena;
	RowOperationsState row_state;
	idx_t non_null_position = 0;
	idx_t non_null_count = 0;
	idx_t current_probe_input = 0;
	idx_t current_entry = DConstants::INVALID_INDEX;
	bool partial_matcher_initialized = false;
	bool partial_input_initialized = false;
	bool active_probe = false;
};

unique_ptr<OperatorState> PhysicalRecursiveCTEKeyJoin::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<RecursiveCTEKeyJoinState>(context.client, *this);
}

static idx_t SelectNonNullKeys(DataChunk &keys, vector<UnifiedVectorFormat> &formats, SelectionVector &result) {
	for (idx_t key_idx = 0; key_idx < keys.ColumnCount(); key_idx++) {
		keys.data[key_idx].ToUnifiedFormat(formats[key_idx]);
	}
	idx_t result_count = 0;
	for (idx_t row_idx = 0; row_idx < keys.size(); row_idx++) {
		bool valid = true;
		for (auto &format : formats) {
			valid = valid && format.validity.RowIsValid(format.sel->get_index(row_idx));
		}
		if (valid) {
			result.set_index(result_count++, row_idx);
		}
	}
	return result_count;
}

RecursiveCTEKeyJoinResult RecursiveCTEKeyJoinState::ProbeCompleteKey(DataChunk &input,
                                                                     const RecursiveCTEKeyJoinLayout &layout,
                                                                     RecursiveCTEState &recursive_state) {
	const auto &probe_key_indices = layout.ProbeKeyIndices();
	probe_keys.Reset();
	for (idx_t key_idx = 0; key_idx < probe_key_indices.size(); key_idx++) {
		probe_keys.data[key_idx].Reference(input.data[probe_key_indices[key_idx]]);
	}
	probe_keys.CheckCardinality(input.size());
	const auto current_non_null_count = SelectNonNullKeys(probe_keys, key_formats, non_null_sel);
	if (current_non_null_count == 0) {
		return {0, OperatorResultType::NEED_MORE_INPUT};
	}

	lookup_keys.Reset();
	if (current_non_null_count == input.size()) {
		lookup_keys.Reference(probe_keys);
	} else {
		lookup_keys.Slice(probe_keys, non_null_sel, current_non_null_count);
	}
	const auto match_count = recursive_state.ht->LookupGroups(lookup_keys, lookup_state, found_key_sel);
	if (recursive_state.metrics.Enabled()) {
		recursive_state.metrics.RecordDirectProbeRows(current_non_null_count);
		recursive_state.metrics.RecordDirectProbeMatches(match_count);
	}
	if (match_count == 0) {
		return {0, OperatorResultType::NEED_MORE_INPUT};
	}

	matched_addresses.SetVectorType(VectorType::FLAT_VECTOR);
	auto matched_address_data = FlatVector::GetDataMutable<data_ptr_t>(matched_addresses);
	auto lookup_addresses = FlatVector::GetData<data_ptr_t>(lookup_state.addresses);
	for (idx_t match_idx = 0; match_idx < match_count; match_idx++) {
		const auto lookup_idx = found_key_sel.get_index_unsafe(match_idx);
		const auto input_idx =
		    current_non_null_count == input.size() ? lookup_idx : non_null_sel.get_index_unsafe(lookup_idx);
		matched_input_sel.set_index(match_idx, input_idx);
		matched_address_data[match_idx] = lookup_addresses[lookup_idx];
	}
	FlatVector::SetSize(matched_addresses, match_count);
	recursive_state.ht->GatherGroups(lookup_state, found_key_sel, match_count, state_keys);
	return {match_count, OperatorResultType::NEED_MORE_INPUT};
}

RecursiveCTEKeyJoinResult RecursiveCTEKeyJoinState::ProbePartialKey(DataChunk &input,
                                                                    const RecursiveCTEKeyJoinLayout &layout,
                                                                    RecursiveCTEState &recursive_state) {
	const auto &state_key_indices = layout.StateKeyIndices();
	const auto &probe_key_indices = layout.ProbeKeyIndices();
	auto &index = recursive_state.GetPartialKeyIndex(state_key_indices);
	if (!partial_input_initialized) {
		probe_keys.Reset();
		for (idx_t key_idx = 0; key_idx < probe_key_indices.size(); key_idx++) {
			probe_keys.data[key_idx].Reference(input.data[probe_key_indices[key_idx]]);
		}
		probe_keys.CheckCardinality(input.size());
		non_null_count = SelectNonNullKeys(probe_keys, key_formats, non_null_sel);
		if (non_null_count == 0) {
			return {0, OperatorResultType::NEED_MORE_INPUT};
		}
		probe_keys.Hash(probe_hashes);
		non_null_position = 0;
		active_probe = false;
		partial_input_initialized = true;
		if (recursive_state.metrics.Enabled()) {
			recursive_state.metrics.RecordDirectProbeRows(non_null_count);
		}
	}
	if (!partial_matcher_initialized) {
		vector<ExpressionType> predicates(state_key_indices.size(), ExpressionType::COMPARE_EQUAL);
		vector<column_t> columns;
		for (auto key_idx : state_key_indices) {
			columns.push_back(key_idx);
		}
		TupleDataCollection::InitializeChunkState(match_chunk_state, layout.KeyTypes(), columns);
		partial_matcher.Initialize(false, recursive_state.ht->GetLayout(), predicates, std::move(columns));
		partial_matcher_initialized = true;
	}

	while (true) {
		idx_t candidate_count = 0;
		candidate_addresses.SetVectorType(VectorType::FLAT_VECTOR);
		auto candidate_address_data = FlatVector::GetDataMutable<data_ptr_t>(candidate_addresses);
		const auto hash_data = probe_hashes.Values<hash_t>();
		while (candidate_count < STANDARD_VECTOR_SIZE) {
			if (!active_probe) {
				if (non_null_position >= non_null_count) {
					break;
				}
				current_probe_input = non_null_sel.get_index_unsafe(non_null_position++);
				current_entry = index.GetHead(hash_data[current_probe_input].GetValue());
				active_probe = true;
			}
			if (current_entry == DConstants::INVALID_INDEX) {
				active_probe = false;
				continue;
			}
			const auto &entry = index.GetEntry(current_entry);
			current_entry = entry.next;
			if (recursive_state.metrics.Enabled()) {
				recursive_state.metrics.RecordPartialProbeChainVisit();
			}
			if (entry.hash != hash_data[current_probe_input].GetValue()) {
				continue;
			}
			candidate_input_sel.set_index(candidate_count, current_probe_input);
			candidate_address_data[candidate_count++] = entry.address;
		}
		if (active_probe && current_entry == DConstants::INVALID_INDEX) {
			active_probe = false;
		}
		const bool has_more = active_probe || non_null_position < non_null_count;
		if (candidate_count == 0) {
			partial_input_initialized = false;
			return {0, OperatorResultType::NEED_MORE_INPUT};
		}
		FlatVector::SetSize(candidate_addresses, candidate_count);
		candidate_keys.Reset();
		for (idx_t partial_idx = 0; partial_idx < state_key_indices.size(); partial_idx++) {
			const auto state_key_idx = state_key_indices[partial_idx];
			candidate_keys.data[state_key_idx].Slice(probe_keys.data[partial_idx], candidate_input_sel,
			                                         candidate_count);
		}
		candidate_keys.SetChildCardinality(candidate_count);
		TupleDataCollection::ToUnifiedFormat(match_chunk_state, candidate_keys);
		for (idx_t candidate_idx = 0; candidate_idx < candidate_count; candidate_idx++) {
			candidate_match_sel.set_index(candidate_idx, candidate_idx);
		}
		idx_t no_match_count = 0;
		const auto match_count =
		    partial_matcher.Match(candidate_keys, match_chunk_state.vector_data, candidate_match_sel, candidate_count,
		                          candidate_addresses, nullptr, no_match_count);
		if (match_count == 0 && has_more) {
			continue;
		}
		if (match_count == 0) {
			partial_input_initialized = false;
			return {0, OperatorResultType::NEED_MORE_INPUT};
		}

		matched_addresses.SetVectorType(VectorType::FLAT_VECTOR);
		auto matched_address_data = FlatVector::GetDataMutable<data_ptr_t>(matched_addresses);
		for (idx_t match_idx = 0; match_idx < match_count; match_idx++) {
			const auto candidate_idx = candidate_match_sel.get_index_unsafe(match_idx);
			matched_input_sel.set_index(match_idx, candidate_input_sel.get_index_unsafe(candidate_idx));
			matched_address_data[match_idx] = candidate_address_data[candidate_idx];
		}
		FlatVector::SetSize(matched_addresses, match_count);
		recursive_state.ht->GatherGroups(lookup_state, matched_addresses, *FlatVector::IncrementalSelectionVector(),
		                                 match_count, state_keys);
		if (recursive_state.metrics.Enabled()) {
			recursive_state.metrics.RecordDirectProbeMatches(match_count);
		}
		if (!has_more) {
			partial_input_initialized = false;
		}
		return {match_count, has_more ? OperatorResultType::HAVE_MORE_OUTPUT : OperatorResultType::NEED_MORE_INPUT};
	}
}

void RecursiveCTEKeyJoinState::FinalizePayload(const RecursiveCTEKeyJoinLayout &layout,
                                               RecursiveCTEState &recursive_state, idx_t match_count) {
	payload_rows.Reset();
	payload_rows.SetChildCardinality(match_count);
	if (layout.PayloadTypes().empty()) {
		return;
	}
	lock_guard<mutex> guard(recursive_state.ht_finalize_lock);
	auto row_layout = recursive_state.ht->GetLayoutPtr();
	RowOperations::FinalizeStates(row_state, *row_layout, matched_addresses, payload_rows, 0);
}

void RecursiveCTEKeyJoinState::EmitResult(DataChunk &input, DataChunk &output, const RecursiveCTEKeyJoinLayout &layout,
                                          idx_t match_count) {
	idx_t output_idx = 0;
	auto emit_probe = [&](const vector<idx_t> &projection_map) {
		for (auto probe_idx : projection_map) {
			output.data[output_idx++].Slice(input.data[probe_idx], matched_input_sel, match_count);
		}
	};
	auto emit_state = [&](const vector<idx_t> &projection_map) {
		for (auto state_idx : projection_map) {
			const auto key_idx = layout.StateKeyMap()[state_idx];
			if (key_idx != DConstants::INVALID_INDEX) {
				output.data[output_idx++].Reference(state_keys.data[key_idx]);
				continue;
			}
			const auto payload_idx = layout.StatePayloadMap()[state_idx];
			D_ASSERT(payload_idx != DConstants::INVALID_INDEX);
			output.data[output_idx++].Reference(payload_rows.data[payload_idx]);
		}
	};
	if (layout.StateOnLeft()) {
		emit_state(layout.LeftProjectionMap());
		emit_probe(layout.RightProjectionMap());
	} else {
		emit_probe(layout.LeftProjectionMap());
		emit_state(layout.RightProjectionMap());
	}
	if (output_idx != output.ColumnCount()) {
		throw InternalException("USING KEY direct probe produced %d columns, expected %d", output_idx,
		                        output.ColumnCount());
	}
	output.CheckCardinality(match_count);
}

OperatorResultType RecursiveCTEKeyJoinState::Execute(DataChunk &input, DataChunk &output,
                                                     const RecursiveCTEKeyJoinLayout &layout,
                                                     RecursiveCTEState &recursive_state) {
	auto result = layout.IsPartial() ? ProbePartialKey(input, layout, recursive_state)
	                                 : ProbeCompleteKey(input, layout, recursive_state);
	if (result.match_count == 0) {
		return result.result_type;
	}
	FinalizePayload(layout, recursive_state, result.match_count);
	EmitResult(input, output, layout, result.match_count);
	return result.result_type;
}

OperatorResultType PhysicalRecursiveCTEKeyJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                                DataChunk &chunk, GlobalOperatorState &gstate,
                                                                OperatorState &state_p) const {
	auto &state_scan = layout.StateScan();
	if (!state_scan.recursive_cte || !state_scan.recursive_cte->sink_state) {
		throw InternalException("USING KEY direct probe has no recursive state");
	}
	auto &recursive_state = state_scan.recursive_cte->sink_state->Cast<RecursiveCTEState>();
	return state_p.Cast<RecursiveCTEKeyJoinState>().Execute(input, chunk, layout, recursive_state);
}

string PhysicalRecursiveCTEKeyJoin::GetName() const {
	return layout.IsPartial() ? "RECURSIVE_PARTIAL_KEY_JOIN" : "RECURSIVE_KEY_JOIN";
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTEKeyJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = "INNER";
	result["Key Columns"] = to_string(layout.ProbeKeyIndices().size());
	result["Key Mode"] = layout.IsPartial() ? "PARTIAL" : "COMPLETE";
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
