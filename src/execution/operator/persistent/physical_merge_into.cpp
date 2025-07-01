#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

PhysicalMergeInto::PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                     map<MergeActionCondition, vector<unique_ptr<MergeIntoOperator>>> actions_p,
                                     idx_t row_id_index)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::MERGE_INTO, std::move(types), 1),
      row_id_index(row_id_index) {
	for (auto &entry : actions_p) {
		MergeActionRange range;
		range.condition = entry.first;
		range.start = actions.size();
		for (auto &action : entry.second) {
			actions.push_back(std::move(action));
		}
		range.end = actions.size();
		action_ranges.emplace(entry.first, range);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct MergeLocalExecutionState {
	unique_ptr<LocalSinkState> local_state;
	unique_ptr<ExpressionExecutor> executor;
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
				state.executor = make_uniq<ExpressionExecutor>(context.client, *action->condition);
			}
			states.push_back(std::move(state));
		}
	}

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

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, MergeIntoLocalState &local_state,
	                    OperatorSinkInput &input, MergeActionRange range) {
		SelectionVector current_sel;
		idx_t current_count = chunk.size();
		for (idx_t i = range.start; i < range.end && current_count > 0; i++) {
			auto &action = op.actions[i];
			auto &local_action_state = local_state.states[i];
			DataChunk new_chunk;
			if (action->condition) {
				auto &executor = *local_action_state.executor;
				SelectionVector selected_sel(chunk.size());
				SelectionVector remaining_sel(chunk.size());
				idx_t match_count =
				    executor.SelectExpression(chunk, selected_sel, remaining_sel, current_sel, current_count);
				if (match_count == 0) {
					// no matches - move to next action
					continue;
				}
				// slice the chunk for this action with the matching sel
				new_chunk.Initialize(context.client, chunk.GetTypes());
				new_chunk.Slice(chunk, selected_sel, match_count);

				// for the next chunk - update the matches
				current_count = current_count - match_count;
				current_sel.Initialize(remaining_sel);
			}
			if (!action->op) {
				if (action->action_type == MergeActionType::MERGE_ERROR) {
					// abort - generate an error message
					string merge_condition;
					merge_condition += EnumUtil::ToString(range.condition);
					if (action->condition) {
						merge_condition += " AND " + action->condition->ToString();
					}
					throw ConstraintException("Merge abort condition %s", merge_condition);
				}
				D_ASSERT(action->action_type == MergeActionType::MERGE_DO_NOTHING);
				continue;
			}
			auto &input_chunk = action->condition ? new_chunk : chunk;
			auto &gstate = sink_states[i];
			auto &lstate = *local_action_state.local_state;
			OperatorSinkInput sink_input {*gstate, lstate, input.interrupt_state};
			SinkResultType result;
			if (!action->expressions.empty()) {
				ExpressionExecutor executor(context.client, action->expressions);

				vector<LogicalType> insert_types;
				for (auto &expr : action->expressions) {
					insert_types.push_back(expr->return_type);
				}
				DataChunk insert_chunk;
				insert_chunk.Initialize(context.client, insert_types);
				executor.Execute(input_chunk, insert_chunk);
				result = action->op->Sink(context, insert_chunk, sink_input);
			} else {
				result = action->op->Sink(context, input_chunk, sink_input);
			}
			if (result == SinkResultType::BLOCKED) {
				throw InternalException("FIXME: SINK blocked");
			}
			local_state.merged_count += input_chunk.size();
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

MergeActionRange PhysicalMergeInto::GetRange(MergeActionCondition condition) const {
	auto entry = action_ranges.find(condition);
	if (entry == action_ranges.end()) {
		// no actions - return empty range
		MergeActionRange range;
		range.condition = condition;
		range.start = 0;
		range.end = 0;
		return range;
	}
	return entry->second;
}

SinkResultType PhysicalMergeInto::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	auto &local_state = input.local_state.Cast<MergeIntoLocalState>();

	// for each row, figure out if we have generated a match or not
	SelectionVector matched(STANDARD_VECTOR_SIZE);
	SelectionVector not_matched(STANDARD_VECTOR_SIZE);

	UnifiedVectorFormat row_id_data;
	chunk.data[row_id_index].ToUnifiedFormat(chunk.size(), row_id_data);

	idx_t matched_count = 0;
	idx_t not_matched_count = 0;
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = row_id_data.sel->get_index(i);
		if (row_id_data.validity.RowIsValid(idx)) {
			// match
			matched.set_index(matched_count++, i);
		} else {
			// no match
			not_matched.set_index(not_matched_count++, i);
		}
	}

	// now slice and call sink for the relevant part
	// FIXME: deal with BLOCKED in Sink
	if (matched_count > 0) {
		DataChunk matched_chunk;
		matched_chunk.Initialize(context.client, chunk.GetTypes());
		matched_chunk.Slice(chunk, matched, matched_count);
		global_state.Sink(context, matched_chunk, local_state, input, GetRange(MergeActionCondition::WHEN_MATCHED));
	}
	if (not_matched_count > 0) {
		DataChunk not_matched_chunk;
		not_matched_chunk.Initialize(context.client, chunk.GetTypes());
		not_matched_chunk.Slice(chunk, not_matched, not_matched_count);
		global_state.Sink(context, not_matched_chunk, local_state, input,
		                  GetRange(MergeActionCondition::WHEN_NOT_MATCHED_BY_TARGET));
	}

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
unique_ptr<GlobalSourceState> PhysicalMergeInto::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

SourceResultType PhysicalMergeInto::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<MergeIntoGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.merged_count.load())));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
