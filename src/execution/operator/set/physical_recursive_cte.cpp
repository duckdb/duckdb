#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/storage/buffer_manager.hpp"

using namespace std;

namespace duckdb {

class PhysicalRecursiveCTEState : public PhysicalOperatorState {
public:
	PhysicalRecursiveCTEState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), top_done(false) {
	}
	unique_ptr<PhysicalOperatorState> top_state;
	unique_ptr<PhysicalOperatorState> bottom_state;
	unique_ptr<GroupedAggregateHashTable> ht;

	bool top_done = false;

	bool recursing = false;
	bool intermediate_empty = true;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
                                           unique_ptr<PhysicalOperator> bottom)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, move(types)), union_all(union_all) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

// first exhaust non recursive term, then exhaust recursive term iteratively until no (new) rows are generated.
void PhysicalRecursiveCTE::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                            PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_);

	if (!state->ht) {
		state->ht = make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context.client),
		                                                   STANDARD_VECTOR_SIZE * 2, types, vector<LogicalType>(),
		                                                   vector<BoundAggregateExpression *>());
	}

	if (!state->recursing) {
		do {
			children[0]->GetChunk(context, chunk, state->top_state.get());
			if (!union_all) {
				idx_t match_count = ProbeHT(chunk, state);
				if (match_count > 0) {
					working_table->Append(chunk);
				}
			} else {
				working_table->Append(chunk);
			}

			if (chunk.size() != 0) {
				return;
			}
		} while (chunk.size() != 0);
		ExecuteRecursivePipelines(context);
		state->recursing = true;
	}

	while (true) {
		children[1]->GetChunk(context, chunk, state->bottom_state.get());

		if (chunk.size() == 0) {
			// Done if there is nothing in the intermediate table
			if (state->intermediate_empty) {
				state->finished = true;
				break;
			}

			working_table->count = 0;
			working_table->chunks.clear();

			working_table->count = intermediate_table.count;
			working_table->chunks = move(intermediate_table.chunks);

			intermediate_table.count = 0;
			intermediate_table.chunks.clear();

			ExecuteRecursivePipelines(context);
			state->bottom_state = children[1]->GetOperatorState();

			state->intermediate_empty = true;
			continue;
		}

		if (!union_all) {
			// If we evaluate using UNION semantics, we have to eliminate duplicates before appending them to
			// intermediate tables.
			idx_t match_count = ProbeHT(chunk, state);
			if (match_count > 0) {
				intermediate_table.Append(chunk);
				state->intermediate_empty = false;
			}
		} else {
			intermediate_table.Append(chunk);
			state->intermediate_empty = false;
		}

		return;
	}
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) {
	for (auto &pipeline : pipelines) {
		pipeline->Reset(context.client);
		pipeline->Execute(context.task);
		pipeline->FinishTask();
	}
}

void PhysicalRecursiveCTE::FinalizePipelines() {
	// re-order the pipelines such that they are executed in the correct order of dependencies
	for (idx_t i = 0; i < pipelines.size(); i++) {
		auto &deps = pipelines[i]->GetDependencies();
		for (idx_t j = i + 1; j < pipelines.size(); j++) {
			if (deps.find(pipelines[j].get()) != deps.end()) {
				// pipeline "i" depends on pipeline "j" but pipeline "i" is scheduled to be executed before pipeline "j"
				std::swap(pipelines[i], pipelines[j]);
				i--;
				continue;
			}
		}
	}
	for (idx_t i = 0; i < pipelines.size(); i++) {
		pipelines[i]->ClearParents();
	}
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_);

	Vector dummy_addresses(LogicalType::POINTER);

	// Use the HT to eliminate duplicate rows
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	idx_t new_group_count = state->ht->FindOrCreateGroups(chunk, dummy_addresses, new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(new_groups, new_group_count);

	return new_group_count;
}

unique_ptr<PhysicalOperatorState> PhysicalRecursiveCTE::GetOperatorState() {
	auto state = make_unique<PhysicalRecursiveCTEState>(*this);
	state->top_state = children[0]->GetOperatorState();
	state->bottom_state = children[1]->GetOperatorState();
	return (move(state));
}

} // namespace duckdb
