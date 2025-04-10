#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"
namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality),
      is_estimated(false) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	static constexpr int64_t NUM_CHUNK_FOR_CHECK = 32;

public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE), is_checked(false) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;

	atomic<bool> is_checked;
	int64_t num_chunk = 0;
	uint64_t num_received = 0;
	uint64_t num_sent = 0;
	double selectivity = 0.0;

public:
	void CheckBFSelectivity(uint64_t num_in, uint64_t num_out) {
		num_received += num_in;
		num_sent += num_out;
		num_chunk++;

		if (num_chunk > NUM_CHUNK_FOR_CHECK) {
			is_checked = true;
			selectivity = static_cast<double>(num_sent) / static_cast<double>(num_received);
		}
	}

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);
	if (result_count == input.size()) {
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
		chunk.Slice(input, state.sel, result_count);
	}

	if (!state.is_checked) {
		state.CheckBFSelectivity(input.size(), result_count);

		if (state.is_checked && !is_estimated) {
			is_estimated = true;
			filter_selectivity = state.selectivity;
		}
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

InsertionOrderPreservingMap<string> PhysicalFilter::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["__expression__"] = expression->GetName();
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
