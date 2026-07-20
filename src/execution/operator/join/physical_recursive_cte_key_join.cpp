#include "duckdb/execution/operator/join/physical_recursive_cte_key_join.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

PhysicalRecursiveCTEKeyJoin::PhysicalRecursiveCTEKeyJoin(
    PhysicalPlan &physical_plan, LogicalComparisonJoin &op, PhysicalOperator &probe,
    PhysicalRecursiveCTEStateScan &state_scan_p, bool state_on_left_p, vector<idx_t> probe_key_indices_p,
    vector<idx_t> left_projection_map_p, vector<idx_t> right_projection_map_p, idx_t estimated_cardinality)
    : CachingPhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_KEY_JOIN, op.types, estimated_cardinality),
      state_scan(state_scan_p), state_on_left(state_on_left_p), probe_key_indices(std::move(probe_key_indices_p)),
      left_projection_map(std::move(left_projection_map_p)), right_projection_map(std::move(right_projection_map_p)),
      state_key_map(state_scan.GetTypes().size(), DConstants::INVALID_INDEX),
      state_payload_map(state_scan.GetTypes().size(), DConstants::INVALID_INDEX) {
	children.push_back(probe);
	for (idx_t key_idx = 0; key_idx < state_scan.distinct_idx.size(); key_idx++) {
		const auto state_idx = state_scan.distinct_idx[key_idx];
		state_key_map[state_idx] = key_idx;
		key_types.push_back(state_scan.GetTypes()[state_idx]);
	}
	for (idx_t payload_idx = 0; payload_idx < state_scan.payload_idx.size(); payload_idx++) {
		const auto state_idx = state_scan.payload_idx[payload_idx];
		state_payload_map[state_idx] = payload_idx;
		payload_types.push_back(state_scan.GetTypes()[state_idx]);
	}
}

class RecursiveCTEKeyJoinState : public CachingOperatorState {
public:
	RecursiveCTEKeyJoinState(ClientContext &context, const PhysicalRecursiveCTEKeyJoin &op)
	    : non_null_sel(STANDARD_VECTOR_SIZE), found_key_sel(STANDARD_VECTOR_SIZE),
	      matched_input_sel(STANDARD_VECTOR_SIZE), matched_addresses(LogicalType::POINTER),
	      key_formats(op.key_types.size()), arena(Allocator::Get(context)), row_state(arena) {
		probe_keys.Initialize(Allocator::Get(context), op.key_types);
		lookup_keys.Initialize(Allocator::Get(context), op.key_types);
		state_keys.Initialize(Allocator::Get(context), op.key_types);
		payload_rows.Initialize(Allocator::Get(context), op.payload_types);
	}

	DataChunk probe_keys;
	DataChunk lookup_keys;
	DataChunk state_keys;
	DataChunk payload_rows;
	SelectionVector non_null_sel;
	SelectionVector found_key_sel;
	SelectionVector matched_input_sel;
	Vector matched_addresses;
	vector<UnifiedVectorFormat> key_formats;
	AggregateHTLookupState lookup_state;
	ArenaAllocator arena;
	RowOperationsState row_state;

	bool SupportsReuse() const override {
		return true;
	}

	void Reset() override {
		ResetCachingState();
	}
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

OperatorResultType PhysicalRecursiveCTEKeyJoin::ExecuteInternal(ExecutionContext &context, DataChunk &input,
                                                                DataChunk &chunk, GlobalOperatorState &gstate,
                                                                OperatorState &state_p) const {
	if (!state_scan.recursive_cte || !state_scan.recursive_cte->sink_state) {
		throw InternalException("USING KEY direct probe has no recursive state");
	}
	auto &recursive_state = state_scan.recursive_cte->sink_state->Cast<RecursiveCTEState>();
	auto &state = state_p.Cast<RecursiveCTEKeyJoinState>();

	state.probe_keys.Reset();
	for (idx_t key_idx = 0; key_idx < probe_key_indices.size(); key_idx++) {
		state.probe_keys.data[key_idx].Reference(input.data[probe_key_indices[key_idx]]);
	}
	state.probe_keys.SetCardinalityUnsafe(input.size());
	const auto non_null_count = SelectNonNullKeys(state.probe_keys, state.key_formats, state.non_null_sel);
	if (non_null_count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.lookup_keys.Reset();
	if (non_null_count == input.size()) {
		state.lookup_keys.Reference(state.probe_keys);
	} else {
		state.lookup_keys.Slice(state.probe_keys, state.non_null_sel, non_null_count);
	}
	const auto match_count =
	    recursive_state.ht->LookupGroups(state.lookup_keys, state.lookup_state, state.found_key_sel);
	if (recursive_state.collect_runtime_metrics) {
		recursive_state.cumulative_direct_probe_rows.fetch_add(non_null_count);
		recursive_state.cumulative_direct_probe_matches.fetch_add(match_count);
	}
	if (match_count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.matched_addresses.SetVectorType(VectorType::FLAT_VECTOR);
	auto matched_addresses = FlatVector::GetDataMutable<data_ptr_t>(state.matched_addresses);
	auto lookup_addresses = FlatVector::GetData<data_ptr_t>(state.lookup_state.addresses);
	for (idx_t match_idx = 0; match_idx < match_count; match_idx++) {
		const auto lookup_idx = state.found_key_sel.get_index_unsafe(match_idx);
		const auto input_idx =
		    non_null_count == input.size() ? lookup_idx : state.non_null_sel.get_index_unsafe(lookup_idx);
		state.matched_input_sel.set_index(match_idx, input_idx);
		matched_addresses[match_idx] = lookup_addresses[lookup_idx];
	}
	FlatVector::SetSize(state.matched_addresses, match_count);
	recursive_state.ht->GatherGroups(state.lookup_state, state.found_key_sel, match_count, state.state_keys);

	state.payload_rows.Reset();
	state.payload_rows.SetCardinalityUnsafe(match_count);
	if (!payload_types.empty()) {
		lock_guard<mutex> guard(recursive_state.ht_finalize_lock);
		auto layout = recursive_state.ht->GetLayoutPtr();
		RowOperations::FinalizeStates(state.row_state, *layout, state.matched_addresses, state.payload_rows, 0);
	}

	idx_t output_idx = 0;
	auto emit_probe = [&](const vector<idx_t> &projection_map) {
		for (auto probe_idx : projection_map) {
			chunk.data[output_idx++].Slice(input.data[probe_idx], state.matched_input_sel, match_count);
		}
	};
	auto emit_state = [&](const vector<idx_t> &projection_map) {
		for (auto state_idx : projection_map) {
			const auto key_idx = state_key_map[state_idx];
			if (key_idx != DConstants::INVALID_INDEX) {
				chunk.data[output_idx++].Reference(state.state_keys.data[key_idx]);
				continue;
			}
			const auto payload_idx = state_payload_map[state_idx];
			D_ASSERT(payload_idx != DConstants::INVALID_INDEX);
			chunk.data[output_idx++].Reference(state.payload_rows.data[payload_idx]);
		}
	};
	if (state_on_left) {
		emit_state(left_projection_map);
		emit_probe(right_projection_map);
	} else {
		emit_probe(left_projection_map);
		emit_state(right_projection_map);
	}
	chunk.SetCardinalityUnsafe(match_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalRecursiveCTEKeyJoin::GetName() const {
	return "RECURSIVE_KEY_JOIN";
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTEKeyJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = "INNER";
	result["Key Columns"] = to_string(probe_key_indices.size());
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
