#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"

namespace duckdb {

PhysicalMergeInto::PhysicalMergeInto(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                     vector<unique_ptr<MergeIntoOperator>> when_matched_actions_p,
                                     vector<unique_ptr<MergeIntoOperator>> when_not_matched_actions_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::MERGE_INTO, std::move(types), 1),
      when_matched_actions(std::move(when_matched_actions_p)),
      when_not_matched_actions(std::move(when_not_matched_actions_p)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class MergeIntoLocalState : public LocalSinkState {
public:
	MergeIntoLocalState(ExecutionContext &context, const PhysicalMergeInto &op) {
		for (auto &action : op.when_matched_actions) {
			local_states.push_back(action->op.GetLocalSinkState(context));
		}
		for (auto &action : op.when_not_matched_actions) {
			local_states.push_back(action->op.GetLocalSinkState(context));
		}
	}

	idx_t combine_idx = 0;
	vector<unique_ptr<LocalSinkState>> local_states;
};

class MergeIntoGlobalState : public GlobalSinkState {
public:
	MergeIntoGlobalState(ClientContext &context, const PhysicalMergeInto &op) : op(op) {
		for (auto &action : op.when_matched_actions) {
			sink_states.push_back(action->op.GetGlobalSinkState(context));
		}
		for (auto &action : op.when_not_matched_actions) {
			sink_states.push_back(action->op.GetGlobalSinkState(context));
		}
		merged_count = 0;
	}
	const PhysicalMergeInto &op;
	idx_t finalize_idx = 0;
	vector<unique_ptr<GlobalSinkState>> sink_states;
	atomic<idx_t> merged_count;

	SinkCombineResultType Combine(ExecutionContext &context, MergeIntoLocalState &local_state,
	                              OperatorSinkCombineInput &input) {
		for (; local_state.combine_idx < local_state.local_states.size(); ++local_state.combine_idx) {
			auto &lstate = local_state.local_states[local_state.combine_idx];
			auto &gstate = sink_states[local_state.combine_idx];
			auto &action = local_state.combine_idx < op.when_matched_actions.size()
			                   ? op.when_matched_actions[local_state.combine_idx]
			                   : op.when_not_matched_actions[local_state.combine_idx - op.when_matched_actions.size()];

			OperatorSinkCombineInput combine_input {*gstate, *lstate, input.interrupt_state};
			auto result = action->op.Combine(context, combine_input);
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

			OperatorSinkFinalizeInput finalize_input {*gstate, input.interrupt_state};
			auto result = action->op.Finalize(pipeline, event, context, finalize_input);
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

SinkResultType PhysicalMergeInto::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	throw InternalException("FIXME: SINK");
}

SinkCombineResultType PhysicalMergeInto::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<MergeIntoGlobalState>();
	auto &local_state = input.local_state.Cast<MergeIntoLocalState>();
	return global_state.Combine(context, local_state, input);
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
	return nullptr;
}

SourceResultType PhysicalMergeInto::GetData(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<MergeIntoGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.merged_count.load())));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
