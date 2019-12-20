#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"

using namespace duckdb;
using namespace std;

class PhysicalFilterState : public PhysicalOperatorState {
public:
	PhysicalFilterState(PhysicalOperator *child, Expression &expr)
	    : PhysicalOperatorState(child), executor(expr) {
	}

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
	while(true) {
		children[0]->GetChunk(context, chunk, state->child_state.get());
		if (chunk.size() == 0) {
			return;
		}
		index_t result_count = state->executor.SelectExpression(chunk, chunk.owned_sel_vector);
		if (result_count > 0) {
			for (index_t i = 0; i < chunk.column_count; i++) {
				chunk.data[i].count = result_count;
				chunk.data[i].sel_vector = chunk.owned_sel_vector;
			}
			chunk.sel_vector = chunk.owned_sel_vector;
			return;
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterState>(children[0].get(), *expression);
}

string PhysicalFilter::ExtraRenderInformation() const {
	return expression->GetName();
}
