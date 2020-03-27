#include "duckdb/execution/operator/projection/physical_unnest.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

using namespace duckdb;
using namespace std;

//! The operator state of the window
class PhysicalUnnestOperatorState : public PhysicalOperatorState {
public:
	PhysicalUnnestOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child), parent_position(0), list_position(0), list_length(-1) {
	}

	idx_t parent_position;
	idx_t list_position;
	int64_t list_length = -1;

	DataChunk list_data;
};

// this implements a sorted window functions variant
PhysicalUnnest::PhysicalUnnest(LogicalOperator &op, vector<unique_ptr<Expression>> select_list,
                               PhysicalOperatorType type)
    : PhysicalOperator(type, op.types), select_list(std::move(select_list)) {

	assert(this->select_list.size() > 0);
}

void PhysicalUnnest::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalUnnestOperatorState *>(state_);
	while (true) { // repeat until we actually have produced some rows
		if (state->child_chunk.size() == 0 || state->parent_position >= state->child_chunk.size()) {
			// get the child data
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				return;
			}
			state->parent_position = 0;
			state->list_position = 0;
			state->list_length = -1;

			// get the list data to unnest
			ExpressionExecutor executor;
			vector<TypeId> list_data_types;
			for (auto &exp : select_list) {
				assert(exp->type == ExpressionType::BOUND_UNNEST);
				auto bue = (BoundUnnestExpression *)exp.get();
				list_data_types.push_back(bue->child->return_type);
				executor.AddExpression(*bue->child.get());
			}
			state->list_data.Destroy();
			state->list_data.Initialize(list_data_types);
			executor.Execute(state->child_chunk, state->list_data);

			// paranoia aplenty
			state->child_chunk.Verify();
			state->list_data.Verify();
			assert(state->child_chunk.size() == state->list_data.size());
			assert(state->list_data.column_count() == select_list.size());
		}

		// need to figure out how many times we need to repeat for current row
		if (state->list_length < 0) {
			for (idx_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {
				auto &v = state->list_data.data[col_idx];

				assert(v.type == TypeId::LIST);
				// TODO deal with NULL values here!
				auto list_data = FlatVector::GetData<list_entry_t>(v);
				auto list_entry = list_data[state->parent_position];
				if ((int64_t)list_entry.length > state->list_length) {
					state->list_length = list_entry.length;
				}
			}
		}

		assert(state->list_length >= 0);

		auto this_chunk_len = min((idx_t)STANDARD_VECTOR_SIZE, state->list_length - state->list_position);

		// first cols are from child, last n cols from unnest
		chunk.SetCardinality(this_chunk_len);

		for (idx_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
			auto val = state->child_chunk.data[col_idx].GetValue(state->parent_position);
			chunk.data[col_idx].Reference(val);
		}

		// FIXME do not use GetValue/SetValue here
		// TODO now that list entries are chunk collections, simply scan them!
		for (idx_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {
			auto target_col = col_idx + state->child_chunk.column_count();
			auto &v = state->list_data.data[col_idx];
			auto list_data = FlatVector::GetData<list_entry_t>(v);
			auto list_entry = list_data[state->parent_position];
			auto &child_cc = ListVector::GetEntry(v);

			idx_t i = 0;
			if (list_entry.length > state->list_position) {
				for (i = 0; i < min((idx_t)this_chunk_len, list_entry.length - state->list_position); i++) {
					chunk.data[target_col].SetValue(i,
					                                child_cc.GetValue(0, list_entry.offset + i + state->list_position));
				}
			}
			for (; i < (idx_t)this_chunk_len; i++) {
				chunk.data[target_col].SetValue(i, Value());
			}
		}

		state->list_position += this_chunk_len;
		if ((int64_t)state->list_position == state->list_length) {
			state->parent_position++;
			state->list_length = -1;
			state->list_position = 0;
		}

		chunk.Verify();
		if (chunk.size() > 0) {
			return;
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalUnnest::GetOperatorState() {
	return make_unique<PhysicalUnnestOperatorState>(children[0].get());
}
