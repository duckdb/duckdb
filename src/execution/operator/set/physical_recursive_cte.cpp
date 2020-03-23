#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

using namespace duckdb;
using namespace std;

class PhysicalRecursiveCTEState : public PhysicalOperatorState {
public:
	PhysicalRecursiveCTEState() : PhysicalOperatorState(nullptr), top_done(false) {
	}
	unique_ptr<PhysicalOperatorState> top_state;
	unique_ptr<PhysicalOperatorState> bottom_state;
	unique_ptr<SuperLargeHashTable> ht;

	bool top_done = false;

	bool recursing = false;
	bool intermediate_empty = true;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(LogicalOperator &op, bool union_all, unique_ptr<PhysicalOperator> top,
                                           unique_ptr<PhysicalOperator> bottom)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, op.types), union_all(union_all) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

// first exhaust non recursive term, then exhaust recursive term iteratively until no (new) rows are generated.
void PhysicalRecursiveCTE::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_);

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

			if (chunk.size() != 0)
				return;
		} while (chunk.size() != 0);
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

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalRecursiveCTEState *>(state_);

	Vector dummy_addresses(TypeId::POINTER);

	// Use the HT to eliminate duplicate rows
	SelectionVector new_groups(STANDARD_VECTOR_SIZE);
	idx_t new_group_count = state->ht->FindOrCreateGroups(chunk, dummy_addresses, new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(new_groups, new_group_count);

	return new_group_count;
}

unique_ptr<PhysicalOperatorState> PhysicalRecursiveCTE::GetOperatorState() {
	auto state = make_unique<PhysicalRecursiveCTEState>();
	state->top_state = children[0]->GetOperatorState();
	state->bottom_state = children[1]->GetOperatorState();
	state->ht = make_unique<SuperLargeHashTable>(1024, types, vector<TypeId>(), vector<BoundAggregateExpression *>());
	return (move(state));
}
