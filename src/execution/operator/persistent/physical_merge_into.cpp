#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/statement/merge_into_statement.hpp"

namespace duckdb {

PhysicalMergeInto::PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                     map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions_p,
                                     idx_t row_id_index, optional_idx source_marker, bool parallel_p,
                                     bool return_chunk_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::MERGE_INTO, std::move(types), 1),
      row_id_index(row_id_index), source_marker(source_marker), parallel(parallel_p), return_chunk(return_chunk_p) {
	map<MergeActionCondition, MergeActionRange> ranges;
	for (auto &entry : actions_p) {
		MergeActionRange range;
		range.condition = entry.first;
		range.start = actions.size();
		for (auto &action : entry.second) {
			actions.push_back(std::move(action));
		}
		range.end = actions.size();
		ranges.emplace(entry.first, range);
	}
	match_actions = {MergeActionCondition::WHEN_MATCHED, MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET,
	                 MergeActionCondition::WHEN_NOT_MATCHED_BY_SOURCE};
	for (idx_t i = 0; i < match_actions.size(); i++) {
		auto entry = ranges.find(match_actions[i]);
		MergeActionRange range;
		if (entry != ranges.end()) {
			range = entry->second;
		}
		range.condition = match_actions[i];
		action_ranges.push_back(range);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct MergeSinkState {
	MergeSinkState() : selected_sel(STANDARD_VECTOR_SIZE), remaining_sel(STANDARD_VECTOR_SIZE) {
	}

	bool computed_matches = false;
	bool match_initialized = false;
	idx_t match_idx = 0;
	idx_t index_in_match = 0;
	SelectionVector current_sel;
	SelectionVector selected_sel;
	SelectionVector remaining_sel;
	idx_t current_count;
	unique_ptr<DataChunk> sliced_chunk;
	optional_ptr<DataChunk> input_chunk;
};

struct MergeLocalExecutionState {
	unique_ptr<LocalSinkState> local_state;
	unique_ptr<ExpressionExecutor> condition_executor;
	unique_ptr<ExpressionExecutor> insert_executor;
	unique_ptr<DataChunk> insert_chunk;
};

struct MatchResult {
	MatchResult(ClientContext &context, const vector<LogicalType> &types) : sel(STANDARD_VECTOR_SIZE), count(0) {
		chunk = make_uniq<DataChunk>();
		chunk->Initialize(context, types);
	}

	SelectionVector sel;
	idx_t count;
	unique_ptr<DataChunk> chunk;
};

class MergeIntoLocalState : public LocalSinkState {
public:
	MergeIntoLocalState(ExecutionContext &context, const PhysicalMergeInto &op) {
		for (auto &action : op.actions) {
			MergeLocalExecutionState state;
			if (action->op) {
				state.local_state = action->op->GetLocalSinkState(context);
			}
			if (action->condition) {
				state.condition_executor = make_uniq<ExpressionExecutor>(context.client, *action->condition);
			}
			if (!action->expressions.empty()) {
				state.insert_executor = make_uniq<ExpressionExecutor>(context.client, action->expressions);
				vector<LogicalType> insert_types;
				for (auto &expr : action->expressions) {
					insert_types.push_back(expr->return_type);
				}
				state.insert_chunk = make_uniq<DataChunk>();
				state.insert_chunk->Initialize(context.client, insert_types);
			}

			states.push_back(std::move(state));
		}
		for (idx_t i = 0; i < 3; i++) {
			match_results.emplace_back(context.client, op.children[0].get().types);
		}
	}

	MergeSinkState sink_state;
	vector<MatchResult> match_results;
	idx_t combine_idx = 0;
	vector<MergeLocalExecutionState> states;
	idx_t merged_count = 0;
};

class MergeIntoGlobalState : public GlobalSinkState {
public:
	MergeIntoGlobalState(ClientContext &context, const PhysicalMergeInto &op) : op(op) {
		for (auto &action : op.actions) {
			sink_states.push_back(action->op ? action->op->GetGlobalSinkState(context) : nullptr);
		}
		merged_count = 0;
	}
	const PhysicalMergeInto &op;
	idx_t finalize_idx = 0;
	vector<unique_ptr<GlobalSinkState>> sink_states;
	atomic<idx_t> merged_count;

	optional_ptr<DataChunk> ComputeActionInput(ClientContext &context, MergeIntoOperator &action, DataChunk &chunk,
	                                           MergeIntoLocalState &local_state,
	                                           MergeLocalExecutionState &local_action_state) {
		auto &current_count = local_state.sink_state.current_count;
		auto &current_sel = local_state.sink_state.current_sel;
		auto &sliced_chunk = local_state.sink_state.sliced_chunk;
		auto &selected_sel = local_state.sink_state.selected_sel;
		auto &remaining_sel = local_state.sink_state.remaining_sel;
		if (current_count == 0) {
			return nullptr;
		}
		if (!sliced_chunk) {
			sliced_chunk = make_uniq<DataChunk>();
			sliced_chunk->Initialize(context, chunk.GetTypes());
		} else {
			sliced_chunk->Reset();
		}
		optional_ptr<DataChunk> result;
		if (action.condition) {
			// if we have a condition we need to evaluate it
			auto &executor = *local_action_state.condition_executor;
			idx_t match_count =
			    executor.SelectExpression(chunk, selected_sel, remaining_sel, current_sel, current_count);
			if (match_count == 0) {
				// no matches - move to next action
				return nullptr;
			}
			// slice the chunk for this action with the matching sel
			sliced_chunk->Slice(chunk, selected_sel, match_count);
			result = sliced_chunk;

			// for the next chunk - update the matches
			current_count = current_count - match_count;
			current_sel.Initialize(remaining_sel);
		} else if (current_count != chunk.size()) {
			// if we have previously processed rows - remove them
			sliced_chunk->Slice(chunk, current_sel, current_count);
			result = sliced_chunk;
		} else {
			result = chunk;
		}
		// if we have any expressions - execute them to generate the new input chunk
		if (!action.expressions.empty()) {
			auto &insert_chunk = local_action_state.insert_chunk;
			insert_chunk->Reset();
			local_action_state.insert_executor->Execute(*result, *insert_chunk);
			result = insert_chunk.get();
		}
		if (action.op) {
			local_state.merged_count += result->size();
		}
		return result;
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, MergeIntoLocalState &local_state,
	                    OperatorSinkInput &input, MergeActionRange range, idx_t &index_in_match) {
		auto &input_chunk = local_state.sink_state.input_chunk;
		for (; range.start + index_in_match < range.end; index_in_match++) {
			idx_t i = range.start + index_in_match;
			auto &action = op.actions[i];
			auto &local_action_state = local_state.states[i];
			if (!input_chunk) {
				// first time processing this action - compute the input chunk
				input_chunk = ComputeActionInput(context.client, *action, chunk, local_state, local_action_state);
				if (!input_chunk) {
					// no data for this action - move to next action
					continue;
				}
			}
			// process the action
			if (!action->op) {
				if (action->action_type == MergeActionType::MERGE_ERROR) {
					// abort - generate an error message
					string merge_condition;
					merge_condition += MergeIntoStatement::ActionConditionToString(range.condition);
					if (action->condition) {
						merge_condition += " AND " + action->condition->ToString();
					}
					if (!action->expressions.empty()) {
						// if there are any user-provided error messages: add the first error message encountered
						merge_condition += ": " + input_chunk->data[0].GetValue(0).ToString();
					}
					throw ConstraintException("Merge error condition %s", merge_condition);
				}
				D_ASSERT(action->action_type == MergeActionType::MERGE_DO_NOTHING);
				input_chunk = nullptr;
				continue;
			}
			auto &gstate = sink_states[i];
			auto &lstate = *local_action_state.local_state;
			OperatorSinkInput sink_input {*gstate, lstate, input.interrupt_state};
			auto result = action->op->Sink(context, *input_chunk, sink_input);
			if (result == SinkResultType::BLOCKED) {
				return SinkResultType::BLOCKED;
			}
			// move to next action
			input_chunk = nullptr;
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkCombineResultType Combine(ExecutionContext &context, MergeIntoLocalState &local_state,
	                              OperatorSinkCombineInput &input) {
		for (; local_state.combine_idx < local_state.states.size(); ++local_state.combine_idx) {
			auto &lstate = local_state.states[local_state.combine_idx];
			auto &gstate = sink_states[local_state.combine_idx];
			auto &action = op.actions[local_state.combine_idx];
			if (!action->op) {
				continue;
			}
			OperatorSinkCombineInput combine_input {*gstate, *lstate.local_state, input.interrupt_state};
			auto result = action->op->Combine(context, combine_input);
			if (result == SinkCombineResultType::BLOCKED) {
				return SinkCombineResultType::BLOCKED;
			}
		}
		return SinkCombineResultType::FINISHED;
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) {
		for (; finalize_idx < sink_states.size(); ++finalize_idx) {
			auto &gstate = sink_states[finalize_idx];
			auto &action = op.actions[finalize_idx];
			if (!action->op) {
				continue;
			}

			OperatorSinkFinalizeInput finalize_input {*gstate, input.interrupt_state};
			auto result = action->op->Finalize(pipeline, event, context, finalize_input);
			if (result == SinkFinalizeType::BLOCKED) {
				return SinkFinalizeType::BLOCKED;
			}
		}
		return SinkFinalizeType::READY;
	}
};

unique_ptr<GlobalSinkState> PhysicalMergeInto::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<MergeIntoGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalMergeInto::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<MergeIntoLocalState>(context, *this);
}

idx_t PhysicalMergeInto::GetIndex(MergeActionCondition condition) const {
	for (idx_t i = 0; i < match_actions.size(); ++i) {
		if (match_actions[i] == condition) {
			return i;
		}
	}
	throw InternalException("Unsupported match action condition");
}

void PhysicalMergeInto::ComputeMatches(MergeIntoLocalState &local_state, DataChunk &chunk) const {
	auto &match_results = local_state.match_results;

	// for each row, figure out if we have generated a match or not
	auto &matched = match_results[0];
	auto &not_matched = match_results[1];
	auto &not_matched_by_source = match_results[2];

	matched.count = 0;
	not_matched.count = 0;
	not_matched_by_source.count = 0;

	UnifiedVectorFormat row_id_data;
	chunk.data[row_id_index].ToUnifiedFormat(chunk.size(), row_id_data);
	if (source_marker.IsValid()) {
		// source marker - check both row id and source marker
		UnifiedVectorFormat source_marker_data;
		chunk.data[source_marker.GetIndex()].ToUnifiedFormat(chunk.size(), source_marker_data);
		for (idx_t i = 0; i < chunk.size(); i++) {
			if (!source_marker_data.validity.RowIsValid(source_marker_data.sel->get_index(i))) {
				// source marker is NULL - no source match
				not_matched_by_source.sel.set_index(not_matched_by_source.count++, i);
			} else if (!row_id_data.validity.RowIsValid(row_id_data.sel->get_index(i))) {
				// target marker is NULL - no target match
				not_matched.sel.set_index(not_matched.count++, i);
			} else {
				// match
				matched.sel.set_index(matched.count++, i);
			}
		}
	} else {
		// no source marker - only check row-ids
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto idx = row_id_data.sel->get_index(i);
			if (row_id_data.validity.RowIsValid(idx)) {
				// match
				matched.sel.set_index(matched.count++, i);
			} else {
				// no match
				not_matched.sel.set_index(not_matched.count++, i);
			}
		}
	}

	// reset and slice chunks
	for (auto &match : match_results) {
		if (match.count == 0) {
			continue;
		}
		match.chunk->Reset();
		match.chunk->Slice(chunk, match.sel, match.count);
	}
}

SinkResultType PhysicalMergeInto::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	auto &local_state = input.local_state.Cast<MergeIntoLocalState>();

	auto &match_results = local_state.match_results;
	auto &computed_matches = local_state.sink_state.computed_matches;
	auto &match_idx = local_state.sink_state.match_idx;
	auto &index_in_match = local_state.sink_state.index_in_match;
	auto &match_initialized = local_state.sink_state.match_initialized;
	auto &current_sel = local_state.sink_state.current_sel;
	auto &current_count = local_state.sink_state.current_count;
	if (!computed_matches) {
		// we haven't figured out which rows have which types of matches - compute them
		ComputeMatches(local_state, chunk);

		// set up the state so we can prepare sinking into the relevant operators
		computed_matches = true;
		match_idx = 0;
		index_in_match = 0;
		match_initialized = false;
	}
	// now slice and call sink for each of the match conditions
	for (; match_idx < 3; match_idx++) {
		auto &match_result = match_results[match_idx];
		if (match_result.count == 0) {
			// no matches for this action
			continue;
		}
		if (!match_initialized) {
			current_sel = SelectionVector();
			current_count = match_result.count;
			match_initialized = true;
		}
		auto match_range_index = GetIndex(match_actions[match_idx]);
		auto result = global_state.Sink(context, *match_result.chunk, local_state, input,
		                                action_ranges[match_range_index], index_in_match);
		if (result == SinkResultType::BLOCKED) {
			return SinkResultType::BLOCKED;
		}
		// move to next match action
		index_in_match = 0;
		match_initialized = false;
	}
	// finished - prepare for next match
	computed_matches = false;
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalMergeInto::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	auto &local_state = input.local_state.Cast<MergeIntoLocalState>();

	auto result = global_state.Combine(context, local_state, input);
	if (result == SinkCombineResultType::FINISHED) {
		global_state.merged_count += local_state.merged_count;
	}
	return result;
}

SinkFinalizeType PhysicalMergeInto::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                             OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	return global_state.Finalize(pipeline, event, context, input);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class MergeGlobalSourceState : public GlobalSourceState {
public:
	explicit MergeGlobalSourceState(ClientContext &context, const PhysicalMergeInto &op) {
		if (!op.return_chunk) {
			return;
		}
		auto &g = op.sink_state->Cast<MergeIntoGlobalState>();
		for (idx_t i = 0; i < op.actions.size(); i++) {
			auto &action = *op.actions[i];
			unique_ptr<GlobalSourceState> global_state;
			if (action.op) {
				// assign the global sink state
				action.op->sink_state = std::move(g.sink_states[i]);
				// initialize the global source state
				global_state = action.op->GetGlobalSourceState(context);
			}
			global_states.push_back(std::move(global_state));
		}
	}

	vector<unique_ptr<GlobalSourceState>> global_states;
};

class MergeLocalSourceState : public LocalSourceState {
public:
	explicit MergeLocalSourceState(ExecutionContext &context, const PhysicalMergeInto &op,
	                               MergeGlobalSourceState &gstate) {
		if (!op.return_chunk) {
			return;
		}
		for (idx_t i = 0; i < op.actions.size(); i++) {
			auto &action = *op.actions[i];
			unique_ptr<LocalSourceState> local_state;
			if (action.op) {
				local_state = action.op->GetLocalSourceState(context, *gstate.global_states[i]);
			}
			local_states.push_back(std::move(local_state));
		}
		vector<LogicalType> scan_types;
		for (idx_t c = 0; c < op.types.size() - 1; c++) {
			scan_types.emplace_back(op.types[c]);
		}
		scan_chunk.Initialize(context.client, scan_types);
	}

	DataChunk scan_chunk;
	vector<unique_ptr<LocalSourceState>> local_states;
	idx_t index = 0;
};

unique_ptr<GlobalSourceState> PhysicalMergeInto::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<MergeGlobalSourceState>(context, *this);
}

unique_ptr<LocalSourceState> PhysicalMergeInto::GetLocalSourceState(ExecutionContext &context,
                                                                    GlobalSourceState &gstate) const {
	return make_uniq<MergeLocalSourceState>(context, *this, gstate.Cast<MergeGlobalSourceState>());
}

SourceResultType PhysicalMergeInto::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<MergeIntoGlobalState>();
	if (!return_chunk) {
		chunk.SetCardinality(1);
		chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.merged_count.load())));
		return SourceResultType::FINISHED;
	}
	auto &gstate = input.global_state.Cast<MergeGlobalSourceState>();
	auto &lstate = input.local_state.Cast<MergeLocalSourceState>();
	chunk.Reset();
	for (; lstate.index < actions.size(); lstate.index++) {
		auto &action = *actions[lstate.index];
		if (!action.op) {
			// no action to scan from
			continue;
		}
		auto &child_gstate = *gstate.global_states[lstate.index];
		auto &child_lstate = *lstate.local_states[lstate.index];
		OperatorSourceInput source_input {child_gstate, child_lstate, input.interrupt_state};

		auto result = action.op->GetData(context, lstate.scan_chunk, source_input);
		if (lstate.scan_chunk.size() > 0) {
			// construct the result chunk
			for (idx_t c = 0; c < lstate.scan_chunk.ColumnCount(); c++) {
				chunk.data[c].Reference(lstate.scan_chunk.data[c]);
			}
			// set the merge action
			string merge_action_name;
			switch (action.action_type) {
			case MergeActionType::MERGE_UPDATE:
				merge_action_name = "UPDATE";
				break;
			case MergeActionType::MERGE_INSERT:
				merge_action_name = "INSERT";
				break;
			case MergeActionType::MERGE_DELETE:
				merge_action_name = "DELETE";
				break;
			default:
				throw InternalException("Unsupported merge action for RETURNING");
			}
			Value merge_action(merge_action_name);
			chunk.data.back().Reference(merge_action);
			chunk.SetCardinality(lstate.scan_chunk.size());
		}

		if (result != SourceResultType::FINISHED) {
			return result;
		}
		if (chunk.size() != 0) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		}
	}
	return SourceResultType::FINISHED;
}

} // namespace duckdb
