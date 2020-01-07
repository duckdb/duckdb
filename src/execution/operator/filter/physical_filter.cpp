#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalFilterState : public PhysicalOperatorState {
public:
	PhysicalFilterState(PhysicalOperator *child, Expression &expr) : PhysicalOperatorState(child), executor(expr) {
	}

	ExpressionExecutor executor;
};

PhysicalFilter::PhysicalFilter(vector<TypeId> types, vector<unique_ptr<Expression>> select_list, vector<index_t> projection_map)
    : PhysicalOperator(PhysicalOperatorType::FILTER, types), projection_map(move(projection_map)) {
	assert(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else {
		expression = move(select_list[0]);
	}

}

PhysicalFilter::PhysicalFilter(vector<TypeId> types, vector<unique_ptr<Expression>> select_list)
    : PhysicalFilter(move(types), move(select_list), vector<index_t>()) {
}

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalFilterState *>(state_);
	DataChunk *child_chunk = projection_map.size() == 0 ? &chunk : &state->child_chunk;
	index_t initial_count;
	index_t result_count;
	do {
		// fetch a chunk from the child and run the filter
		// we repeat this process until either (1) passing tuples are found, or (2) the child is completely exhausted
		children[0]->GetChunk(context, *child_chunk, state->child_state.get());
		if (child_chunk->size() == 0) {
			return;
		}
		initial_count = child_chunk->size();
		result_count = state->executor.SelectExpression(*child_chunk, chunk.owned_sel_vector);
	} while(result_count == 0);

	// found results:
	if (projection_map.size() != 0) {
		// there are columns to project, set up the column references
		assert(chunk.column_count == projection_map.size());
		for(index_t i = 0; i < projection_map.size(); i++) {
			chunk.data[i].Reference(state->child_chunk.data[projection_map[i]]);
		}
		chunk.sel_vector = state->child_chunk.sel_vector;
	}
	if (result_count == initial_count) {
		// nothing was filtered: skip adding any selection vectors
		return;
	}
	for (index_t i = 0; i < chunk.column_count; i++) {
		chunk.data[i].count = result_count;
		chunk.data[i].sel_vector = chunk.owned_sel_vector;
	}
	chunk.sel_vector = chunk.owned_sel_vector;
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterState>(children[0].get(), *expression);
}

string PhysicalFilter::ExtraRenderInformation() const {
	return expression->GetName();
}
