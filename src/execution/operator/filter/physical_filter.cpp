#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalFilterState : public PhysicalOperatorState {
public:
	PhysicalFilterState(PhysicalOperator *child, Expression &expr)
	    : PhysicalOperatorState(child), executor(expr) {
		// initialize the intermediate
		vector<TypeId> result_type = { TypeId::BOOLEAN };
		intermediate.Initialize(result_type);
	}

	DataChunk intermediate;
	ExpressionExecutor executor;
};

PhysicalFilter::PhysicalFilter(vector<TypeId> types, vector<unique_ptr<Expression>> select_list) :
	PhysicalOperator(PhysicalOperatorType::FILTER, types) {
	assert(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for(auto &expr : select_list) {
			conjunction->children.push_back(move(expr));
		}
		expression = move(conjunction);
	} else {
		expression = move(select_list[0]);
	}
}

void PhysicalFilter::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
	auto state = reinterpret_cast<PhysicalFilterState *>(state_);
	do {
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		state->intermediate.Reset();

		state->executor.ExecuteExpression(state->child_chunk, state->intermediate.data[0]);

		// now generate the selection vector
		chunk.sel_vector = state->child_chunk.sel_vector;
		for (index_t i = 0; i < chunk.column_count; i++) {
			// create a reference to the vector of the child chunk
			chunk.data[i].Reference(state->child_chunk.data[i]);
		}
		chunk.SetSelectionVector(state->intermediate.data[0]);
	} while (chunk.size() == 0);
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterState>(children[0].get(), *expression);
}

string PhysicalFilter::ExtraRenderInformation() const {
	return expression->GetName();
}
