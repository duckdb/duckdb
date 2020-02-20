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
	PhysicalUnnestOperatorState(PhysicalOperator *child) : PhysicalOperatorState(child), parent_position(0) {
	}

	index_t parent_position;
	//	index_t list_position;
	//	int64_t list_length = -1;

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

	if (state->child_chunk.size() == 0 || state->parent_position >= state->child_chunk.size()) {
		// get the child data
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		state->parent_position = 0;
		// state->list_position = 0;
		// state->list_length = -1;

		// get the list data to unnest
		ExpressionExecutor executor;
		vector<TypeId> list_data_types;
		for (auto &exp : select_list) {
			assert(exp->type == ExpressionType::BOUND_UNNEST);
			auto bue = (BoundUnnestExpression *)exp.get();
			list_data_types.push_back(bue->child->return_type);
			executor.AddExpression(*bue->child.get());
		}
		state->list_data.Initialize(list_data_types);
		executor.Execute(state->child_chunk, state->list_data);

		// paranoia aplenty
		state->child_chunk.Verify();
		state->list_data.Verify();
		assert(state->child_chunk.size() == state->list_data.size());
		assert(state->list_data.column_count() == select_list.size());
	}

	// FIXME
	int64_t max_list_length = -1;

	// need to figure out how many times we need to repeat for current row
	if (max_list_length < 0) {
		for (index_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {
			auto &v = state->list_data.data[col_idx];

			assert(v.type == TypeId::LIST);
			// TODO deal with NULL values here!

			auto list_entry = ((list_entry_t *)v.GetData())[state->parent_position];
			if ((int64_t)list_entry.length > max_list_length) {
				max_list_length = list_entry.length;
			}
		}
	}

	assert(max_list_length < STANDARD_VECTOR_SIZE); // FIXME this needs to be possible
	assert(max_list_length >= 0);

	// first cols are from child, last n cols from unnest
	state->child_chunk.SetCardinality(max_list_length);

	for (index_t col_idx = 0; col_idx < state->child_chunk.column_count(); col_idx++) {
		VectorOperations::Set(chunk.data[col_idx],
		                      state->child_chunk.data[col_idx].GetValue(state->parent_position));
	}
	for (index_t col_idx = 0; col_idx < state->list_data.column_count(); col_idx++) {
		auto target_col = col_idx + state->child_chunk.column_count();
		chunk.data[target_col].nullmask.all();
		auto &v = state->list_data.data[col_idx];
		auto list_entry = ((list_entry_t *)v.GetData())[state->parent_position];
		auto &child_v = v.GetListEntry();

		for (index_t i = 0; i < list_entry.length; i++) {
			chunk.data[target_col].SetValue(i, child_v.GetValue(list_entry.offset + i));
		}
		for (index_t i = list_entry.length; i < (index_t)max_list_length; i++) {
			chunk.data[target_col].SetValue(i, Value());
		}
	}

	state->parent_position++;
	chunk.Verify();
}

unique_ptr<PhysicalOperatorState> PhysicalUnnest::GetOperatorState() {
	return make_unique<PhysicalUnnestOperatorState>(children[0].get());
}
