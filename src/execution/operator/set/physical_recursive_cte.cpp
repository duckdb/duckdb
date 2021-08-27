#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

// class PhysicalRecursiveCTEState : public OperatorState {
// public:
// 	explicit PhysicalRecursiveCTEState(PhysicalOperator &op) : OperatorState(op, nullptr), top_done(false) {
// 	}
// 	unique_ptr<OperatorState> top_state;
// 	unique_ptr<OperatorState> bottom_state;
// 	unique_ptr<GroupedAggregateHashTable> ht;

// 	bool top_done = false;

// 	bool recursing = false;
// 	bool intermediate_empty = true;
// 	std::shared_ptr<ChunkCollection> working_table;
// 	ChunkCollection intermediate_table;
// };

PhysicalRecursiveCTE::PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
                                           unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, move(types), estimated_cardinality), union_all(union_all) {
	throw InternalException("FIXME: recursive CTE");
	children.push_back(move(top));
	children.push_back(move(bottom));
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

// // first exhaust non recursive term, then exhaust recursive term iteratively until no (new) rows are generated.
// void PhysicalRecursiveCTE::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
//                                             OperatorState *state_p) const {
// 	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_p);

// 	if (!state->ht) {
// 		state->ht = make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context.client), types,
// 		                                                   vector<LogicalType>(), vector<BoundAggregateExpression *>());
// 	}

// 	if (!state->recursing) {
// 		do {
// 			children[0]->GetChunk(context, chunk, state->top_state.get());
// 			if (!union_all) {
// 				idx_t match_count = ProbeHT(chunk, state);
// 				if (match_count > 0) {
// 					state->working_table->Append(chunk);
// 				}
// 			} else {
// 				state->working_table->Append(chunk);
// 			}

// 			if (chunk.size() != 0) {
// 				return;
// 			}
// 		} while (chunk.size() != 0);
// 		ExecuteRecursivePipelines(context);
// 		state->recursing = true;
// 	}

// 	while (true) {
// 		children[1]->GetChunk(context, chunk, state->bottom_state.get());

// 		if (chunk.size() == 0) {
// 			// Done if there is nothing in the intermediate table
// 			if (state->intermediate_empty) {
// 				state->finished = true;
// 				break;
// 			}

// 			state->working_table->Reset();
// 			state->working_table->Merge(state->intermediate_table);
// 			state->intermediate_table.Reset();

// 			ExecuteRecursivePipelines(context);
// 			state->bottom_state = children[1]->GetOperatorState();

// 			state->intermediate_empty = true;
// 			continue;
// 		}

// 		if (!union_all) {
// 			// If we evaluate using UNION semantics, we have to eliminate duplicates before appending them to
// 			// intermediate tables.
// 			idx_t match_count = ProbeHT(chunk, state);
// 			if (match_count > 0) {
// 				state->intermediate_table.Append(chunk);
// 				state->intermediate_empty = false;
// 			} else {
// 				continue;
// 			}
// 		} else {
// 			state->intermediate_table.Append(chunk);
// 			state->intermediate_empty = false;
// 		}

// 		return;
// 	}
// }

// void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
// 	if (pipelines.empty()) {
// 		return;
// 	}

// 	for (auto &pipeline : pipelines) {
// 		pipeline->Reset(context.client);
// 		pipeline->Schedule();
// 	}

// 	// now execute tasks until all pipelines are completed again
// 	auto &scheduler = TaskScheduler::GetScheduler(context.client);
// 	auto &token = pipelines[0]->token;
// 	while (true) {
// 		unique_ptr<Task> task;
// 		while (scheduler.GetTaskFromProducer(token, task)) {
// 			task->Execute();
// 			task.reset();
// 		}
// 		bool finished = true;
// 		for (auto &pipeline : pipelines) {
// 			if (!pipeline->IsFinished()) {
// 				finished = false;
// 				break;
// 			}
// 		}
// 		if (finished) {
// 			// all pipelines finished: done!
// 			break;
// 		}
// 	}
// }

// idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, OperatorState *state_p) const {
// 	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_p);

// 	Vector dummy_addresses(LogicalType::POINTER);

// 	// Use the HT to eliminate duplicate rows
// 	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
// 	idx_t new_group_count = state->ht->FindOrCreateGroups(chunk, dummy_addresses, new_groups);

// 	// we only return entries we have not seen before (i.e. new groups)
// 	chunk.Slice(new_groups, new_group_count);

// 	return new_group_count;
// }

// unique_ptr<OperatorState> PhysicalRecursiveCTE::GetOperatorState() {
// 	auto state = make_unique<PhysicalRecursiveCTEState>(*this);
// 	state->top_state = children[0]->GetOperatorState();
// 	state->bottom_state = children[1]->GetOperatorState();
// 	state->working_table = working_table;
// 	return (move(state));
// }

// void PhysicalRecursiveCTE::FinalizeOperatorState(OperatorState &state_p, ExecutionContext &context) {
// 	auto &state = reinterpret_cast<PhysicalRecursiveCTEState &>(state_p);
// 	if (!children.empty() && state.top_state) {
// 		children[0]->FinalizeOperatorState(*state.top_state, context);
// 	}
// 	if (!children.empty() && state.bottom_state) {
// 		children[1]->FinalizeOperatorState(*state.bottom_state, context);
// 	}
// }

} // namespace duckdb
