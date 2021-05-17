#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
namespace duckdb {

class PhysicalFilterState : public PhysicalOperatorState {
public:
	PhysicalFilterState(PhysicalOperator &op, PhysicalOperator *child, Expression &expr)
	    : PhysicalOperatorState(op, child), executor(expr) {
	}

	ExpressionExecutor executor;
};

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::FILTER, move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
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

void PhysicalFilter::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                      PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalFilterState *>(state_p);
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	idx_t initial_count;
	idx_t result_count;
	do {
		// fetch a chunk from the child and run the filter
		// we repeat this process until either (1) passing tuples are found, or (2) the child is completely exhausted
		children[0]->GetChunk(context, chunk, state->child_state.get());
		if (chunk.size() == 0) {
			return;
		}
		initial_count = chunk.size();
		result_count = state->executor.SelectExpression(chunk, sel);
	} while (result_count == 0);

	if (result_count == initial_count) {
		// nothing was filtered: skip adding any selection vectors
		return;
	}
	chunk.Slice(sel, result_count);
}

unique_ptr<PhysicalOperatorState> PhysicalFilter::GetOperatorState() {
	return make_unique<PhysicalFilterState>(*this, children[0].get(), *expression);
}

string PhysicalFilter::ParamsToString() const {
	return expression->GetName();
}

void PhysicalFilter::FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) {
	auto &state = reinterpret_cast<PhysicalFilterState &>(state_p);
	context.thread.profiler.Flush(this, &state.executor);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}

} // namespace duckdb
