#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

PhysicalMergeInto::PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                     vector<unique_ptr<MergeIntoOperator>> when_matched_actions_p,
                                     vector<unique_ptr<MergeIntoOperator>> when_not_matched_actions_p, idx_t row_id_index)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::MERGE_INTO, std::move(types), 1),
      when_matched_actions(std::move(when_matched_actions_p)),
      when_not_matched_actions(std::move(when_not_matched_actions_p)), row_id_index(row_id_index) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MergeIntoLocalState : public LocalSinkState {
public:
	MergeIntoLocalState(ExecutionContext &context, const PhysicalMergeInto &op) {
		for (auto &action : op.when_matched_actions) {
			local_states.push_back(action->op ? action->op->GetLocalSinkState(context) : nullptr);
		}
		for (auto &action : op.when_not_matched_actions) {
			local_states.push_back(action->op ? action->op->GetLocalSinkState(context) : nullptr);
		}
	}

	idx_t combine_idx = 0;
	vector<unique_ptr<LocalSinkState>> local_states;
	idx_t merged_count = 0;
};

class MergeIntoGlobalState : public GlobalSinkState {
public:
	MergeIntoGlobalState(ClientContext &context, const PhysicalMergeInto &op) : op(op) {
		for (auto &action : op.when_matched_actions) {
			sink_states.push_back(action->op ? action->op->GetGlobalSinkState(context) : nullptr);
		}
		for (auto &action : op.when_not_matched_actions) {
			sink_states.push_back(action->op ? action->op->GetGlobalSinkState(context) : nullptr);
		}
		merged_count = 0;
	}
	const PhysicalMergeInto &op;
	idx_t finalize_idx = 0;
	vector<unique_ptr<GlobalSinkState>> sink_states;
	atomic<idx_t> merged_count;

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, MergeIntoLocalState &local_state,
	OperatorSinkInput &input, bool matched) {
		auto &actions = matched ? op.when_matched_actions : op.when_not_matched_actions;
		idx_t offset = matched ? 0 : op.when_matched_actions.size();
		for(idx_t i = 0; i < actions.size(); i++) {
			auto &action = actions[i];
			if (!action->op) {
				if (action->action_type == MergeActionType::MERGE_ABORT) {
					// abort - generate an error message
					string merge_condition = "WHEN";
					if (!matched) {
						merge_condition += " NOT";
					}
					merge_condition += " MATCHED";
					if (action->condition) {
						merge_condition += " AND " + action->condition->ToString();
					}
					throw ConstraintException("Merge abort condition %s", merge_condition);
				}
				D_ASSERT(action->action_type == MergeActionType::MERGE_DO_NOTHING);
				continue;
			}
			auto &gstate = sink_states[i + offset];
			auto &lstate = local_state.local_states[i + offset];
			OperatorSinkInput sink_input {*gstate, *lstate, input.interrupt_state};
			SinkResultType result;
			if (!action->expressions.empty()) {
				ExpressionExecutor executor(context.client, action->expressions);

				vector<LogicalType> insert_types;
				for(auto &expr : action->expressions) {
					insert_types.push_back(expr->return_type);
				}
				DataChunk insert_chunk;
				insert_chunk.Initialize(context.client, insert_types);
				executor.Execute(chunk, insert_chunk);
				result = action->op->Sink(context, insert_chunk, sink_input);
			} else {
				result = action->op->Sink(context, chunk, sink_input);
			}
			if (result == SinkResultType::BLOCKED) {
				throw InternalException("FIXME: SINK blocked");
			}
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	SinkCombineResultType Combine(ExecutionContext &context, MergeIntoLocalState &local_state,
	                              OperatorSinkCombineInput &input) {
		for (; local_state.combine_idx < local_state.local_states.size(); ++local_state.combine_idx) {
			auto &lstate = local_state.local_states[local_state.combine_idx];
			auto &gstate = sink_states[local_state.combine_idx];
			auto &action = local_state.combine_idx < op.when_matched_actions.size()
			                   ? op.when_matched_actions[local_state.combine_idx]
			                   : op.when_not_matched_actions[local_state.combine_idx - op.when_matched_actions.size()];
			if (!action->op) {
				continue;
			}
			OperatorSinkCombineInput combine_input {*gstate, *lstate, input.interrupt_state};
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
			auto &action = finalize_idx < op.when_matched_actions.size()
			                   ? op.when_matched_actions[finalize_idx]
			                   : op.when_not_matched_actions[finalize_idx - op.when_matched_actions.size()];
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

//	enum class SinkResultType : uint8_t { NEED_MORE_INPUT, FINISHED, BLOCKED };
SinkResultType PhysicalMergeInto::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	auto &local_state = input.local_state.Cast<MergeIntoLocalState>();
	local_state.merged_count += chunk.size();

	// for each row, figure out if we have generated a match or not
	SelectionVector matched(STANDARD_VECTOR_SIZE);
	SelectionVector not_matched(STANDARD_VECTOR_SIZE);

	UnifiedVectorFormat row_id_data;
	chunk.data[row_id_index].ToUnifiedFormat(chunk.size(), row_id_data);

	idx_t matched_count = 0;
	idx_t not_matched_count = 0;
	for(idx_t i = 0; i < chunk.size(); i++) {
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
		global_state.Sink(context, matched_chunk, local_state, input, true);
	}
	if (not_matched_count > 0) {
		DataChunk not_matched_chunk;
		not_matched_chunk.Initialize(context.client, chunk.GetTypes());
		not_matched_chunk.Slice(chunk, not_matched, not_matched_count);
		global_state.Sink(context, not_matched_chunk, local_state, input, false);
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
