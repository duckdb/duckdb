#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

class PhysicalHashJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalHashJoinOperatorState(PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(left), initialized(false) {
		assert(left && right);
	}

	bool initialized;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type) {
	hash_table = make_unique<JoinHashTable>(conditions, right->GetTypes(), join_type);

	children.push_back(move(left));
	children.push_back(move(right));
}

void PhysicalHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinOperatorState *>(state_);
	if (!state->initialized) {
		// build the HT
		auto right_state = children[1]->GetOperatorState();
		auto types = children[1]->GetTypes();

		DataChunk right_chunk;
		right_chunk.Initialize(types);

		state->join_keys.Initialize(hash_table->condition_types);
		while (true) {
			// get the child chunk
			children[1]->GetChunk(context, right_chunk, right_state.get());
			if (right_chunk.size() == 0) {
				break;
			}
			// resolve the join keys for the right chunk
			state->join_keys.Reset();
			ExpressionExecutor executor(right_chunk);
			for (index_t i = 0; i < conditions.size(); i++) {
				executor.ExecuteExpression(*conditions[i].right, state->join_keys.data[i]);
			}
			// build the HT
			hash_table->Build(state->join_keys, right_chunk);
		}

		if (hash_table->size() == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}

		state->initialized = true;
	}
	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		// remove any selection vectors
		state->child_chunk.Flatten();
		if (hash_table->size() == 0) {
			// empty hash table, special case
			if (hash_table->join_type == JoinType::ANTI) {
				// anti join with empty hash table, NOP join
				// return the input
				assert(chunk.column_count == state->child_chunk.column_count);
				for (index_t i = 0; i < chunk.column_count; i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				return;
			} else if (hash_table->join_type == JoinType::MARK) {
				// MARK join with empty hash table
				assert(hash_table->join_type == JoinType::MARK);
				assert(chunk.column_count == state->child_chunk.column_count + 1);
				auto &result_vector = chunk.data[state->child_chunk.column_count];
				assert(result_vector.type == TypeId::BOOLEAN);
				result_vector.count = state->child_chunk.size();
				// for every data vector, we just reference the child chunk
				for (index_t i = 0; i < state->child_chunk.column_count; i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the MARK vector:
				// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
				// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
				// has NULL for every input entry
				if (!hash_table->has_null) {
					auto bool_result = (bool *)result_vector.data;
					for (index_t i = 0; i < result_vector.count; i++) {
						bool_result[i] = false;
					}
				} else {
					result_vector.nullmask.set();
				}
				return;
			}
		}
		// resolve the join keys for the left chunk
		state->join_keys.Reset();
		ExpressionExecutor executor(state->child_chunk);
		for (index_t i = 0; i < conditions.size(); i++) {
			executor.ExecuteExpression(*conditions[i].left, state->join_keys.data[i]);
		}
		// perform the actual probe
		state->scan_structure = hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
	return make_unique<PhysicalHashJoinOperatorState>(children[0].get(), children[1].get());
}
