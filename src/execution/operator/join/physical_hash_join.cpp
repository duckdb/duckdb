#include "duckdb/execution/operator/join/physical_hash_join.hpp"

#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

using namespace duckdb;
using namespace std;

class PhysicalHashJoinState : public PhysicalComparisonJoinState {
public:
	PhysicalHashJoinState(PhysicalOperator *left, PhysicalOperator *right, vector<JoinCondition> &conditions)
	    : PhysicalComparisonJoinState(left, right, conditions), initialized(false) {
	}

	bool initialized;
	DataChunk cached_chunk;
	DataChunk join_keys;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};

PhysicalHashJoin::PhysicalHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   vector<idx_t> left_projection_map, vector<idx_t> right_projection_map)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type),
      right_projection_map(right_projection_map) {
	children.push_back(move(left));
	children.push_back(move(right));

	assert(left_projection_map.size() == 0);

	hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions,
	                               LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map), type);
}

PhysicalHashJoin::PhysicalHashJoin(ClientContext &context, LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type)
    : PhysicalHashJoin(context, op, move(left), move(right), move(cond), join_type, {}, {}) {
}

void PhysicalHashJoin::BuildHashTable(ClientContext &context, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);

	// build the HT
	auto right_state = children[1]->GetOperatorState();
	auto types = children[1]->GetTypes();

	DataChunk right_chunk, build_chunk;
	right_chunk.Initialize(types);

	if (right_projection_map.size() > 0) {
		build_chunk.Initialize(hash_table->build_types);
	}

	state->join_keys.Initialize(hash_table->condition_types);
	while (true) {
		// get the child chunk
		children[1]->GetChunk(context, right_chunk, right_state.get());
		if (right_chunk.size() == 0) {
			break;
		}
		// resolve the join keys for the right chunk
		state->rhs_executor.Execute(right_chunk, state->join_keys);
		// build the HT
		if (right_projection_map.size() > 0) {
			// there is a projection map: fill the build chunk with the projected columns
			build_chunk.Reset();
			build_chunk.SetCardinality(right_chunk);
			for (idx_t i = 0; i < right_projection_map.size(); i++) {
				build_chunk.data[i].Reference(right_chunk.data[right_projection_map[i]]);
			}
			hash_table->Build(state->join_keys, build_chunk);
		} else {
			// there is not a projected map: place the entire right chunk in the HT
			hash_table->Build(state->join_keys, right_chunk);
		}
	}
	hash_table->Finalize();
}

void PhysicalHashJoin::ProbeHashTable(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);
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
		if (hash_table->size() == 0) {
			// empty hash table, special case
			if (hash_table->join_type == JoinType::ANTI) {
				// anti join with empty hash table, NOP join
				// return the input
				assert(chunk.column_count() == state->child_chunk.column_count());
				chunk.Reference(state->child_chunk);
				return;
			} else if (hash_table->join_type == JoinType::MARK) {
				// MARK join with empty hash table
				assert(hash_table->join_type == JoinType::MARK);
				assert(chunk.column_count() == state->child_chunk.column_count() + 1);
				auto &result_vector = chunk.data.back();
				assert(result_vector.type == TypeId::BOOL);
				// for every data vector, we just reference the child chunk
				chunk.SetCardinality(state->child_chunk);
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the MARK vector:
				// if the HT has no NULL values (i.e. empty result set), return a vector that has false for every input
				// entry if the HT has NULL values (i.e. result set had values, but all were NULL), return a vector that
				// has NULL for every input entry
				if (!hash_table->has_null) {
					auto bool_result = FlatVector::GetData<bool>(result_vector);
					for (idx_t i = 0; i < chunk.size(); i++) {
						bool_result[i] = false;
					}
				} else {
					FlatVector::Nullmask(result_vector).set();
				}
				return;
			} else if (hash_table->join_type == JoinType::LEFT || hash_table->join_type == JoinType::OUTER ||
			           hash_table->join_type == JoinType::SINGLE) {
				// LEFT/FULL OUTER/SINGLE join and build side is empty
				// for the LHS we reference the data
				chunk.SetCardinality(state->child_chunk.size());
				for (idx_t i = 0; i < state->child_chunk.column_count(); i++) {
					chunk.data[i].Reference(state->child_chunk.data[i]);
				}
				// for the RHS
				for (idx_t k = state->child_chunk.column_count(); k < chunk.column_count(); k++) {
					chunk.data[k].vector_type = VectorType::CONSTANT_VECTOR;
					ConstantVector::SetNull(chunk.data[k], true);
				}
				return;
			}
		}
		// resolve the join keys for the left chunk
		state->lhs_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		state->scan_structure = hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
	return make_unique<PhysicalHashJoinState>(children[0].get(), children[1].get(), conditions);
}

void PhysicalHashJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_);
	if (!state->initialized) {
		state->cached_chunk.Initialize(types);
		BuildHashTable(context, state_);
		state->initialized = true;

		if (hash_table->size() == 0 &&
		    (hash_table->join_type == JoinType::INNER || hash_table->join_type == JoinType::SEMI)) {
			// empty hash table with INNER or SEMI join means empty result set
			return;
		}
	}
	do {
		ProbeHashTable(context, chunk, state);
#if STANDARD_VECTOR_SIZE >= 128
		if (chunk.size() == 0) {
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
			}
			return;
		} else if (chunk.size() < 64) {
			// small chunk: add it to chunk cache and continue
			state->cached_chunk.Append(chunk);
			if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
				// chunk cache full: return it
				chunk.Reference(state->cached_chunk);
				state->cached_chunk.Reset();
				return;
			} else {
				// chunk cache not full: probe again
				chunk.Reset();
			}
		} else {
			return;
		}
#else
		return;
#endif
	} while (true);
}
