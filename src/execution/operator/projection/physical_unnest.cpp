#include "duckdb/execution/operator/projection/physical_unnest.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_unnest_expression.hpp"

namespace duckdb {

//! The operator state of the window
class PhysicalUnnestOperatorState : public PhysicalOperatorState {
public:
	PhysicalUnnestOperatorState(PhysicalOperator &op, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), parent_position(0), list_position(0), list_length(-1) {
	}

	idx_t parent_position;
	idx_t list_position;
	int64_t list_length = -1;

	DataChunk list_data;
	VectorData list_vector_data;
};

// this implements a sorted window functions variant
PhysicalUnnest::PhysicalUnnest(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(std::move(select_list)) {

	D_ASSERT(!this->select_list.empty());
}

void PhysicalUnnest::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                      PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalUnnestOperatorState *>(state_p);
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
			vector<LogicalType> list_data_types;
			for (auto &exp : select_list) {
				D_ASSERT(exp->type == ExpressionType::BOUND_UNNEST);
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
			D_ASSERT(state->child_chunk.size() == state->list_data.size());
			D_ASSERT(state->list_data.ColumnCount() == select_list.size());

			// initialize VectorData object so the nullmask can accessed
			state->list_data.data[0].Orrify(state->list_data.size(), state->list_vector_data);
		}

		// whether we have UNNEST(*expression returning list that evaluated to NULL*)
		bool unnest_null = !state->list_vector_data.validity.RowIsValid(
		    state->list_vector_data.sel->get_index(state->parent_position));

		// need to figure out how many times we need to repeat for current row
		if (state->list_length < 0) {
			for (idx_t col_idx = 0; col_idx < state->list_data.ColumnCount(); col_idx++) {
				auto &v = state->list_data.data[col_idx];

				D_ASSERT(v.GetType().id() == LogicalTypeId::LIST);

				// deal with NULL values
				if (unnest_null) {
					state->list_length = 1;
					continue;
				}

				auto list_data = FlatVector::GetData<list_entry_t>(v);
				auto list_entry = list_data[state->parent_position];
				if ((int64_t)list_entry.length > state->list_length) {
					state->list_length = list_entry.length;
				}
			}
		}

		D_ASSERT(state->list_length >= 0);

		auto this_chunk_len = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state->list_length - state->list_position);

		// first cols are from child, last n cols from unnest
		chunk.SetCardinality(this_chunk_len);

		for (idx_t col_idx = 0; col_idx < state->child_chunk.ColumnCount(); col_idx++) {
			auto val = state->child_chunk.data[col_idx].GetValue(state->parent_position);
			chunk.data[col_idx].Reference(val);
		}

		// FIXME do not use GetValue/SetValue here
		for (idx_t col_idx = 0; col_idx < state->list_data.ColumnCount(); col_idx++) {
			auto target_col = col_idx + state->child_chunk.ColumnCount();
			auto &v = state->list_data.data[col_idx];
			auto list_data = FlatVector::GetData<list_entry_t>(v);
			auto list_entry = list_data[state->parent_position];
			idx_t i = 0;
			if (list_entry.length > state->list_position) {
				if (unnest_null) {
					for (i = 0; i < MinValue<idx_t>(this_chunk_len, list_entry.length - state->list_position); i++) {
						FlatVector::SetNull(chunk.data[target_col], i, true);
					}
				} else {
					auto &child_vector = ListVector::GetEntry(v);
					for (i = 0; i < MinValue<idx_t>(this_chunk_len, list_entry.length - state->list_position); i++) {
						chunk.data[target_col].SetValue(
						    i, child_vector.GetValue(list_entry.offset + i + state->list_position));
					}
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
	return make_unique<PhysicalUnnestOperatorState>(*this, children[0].get());
}

} // namespace duckdb
