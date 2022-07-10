#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

PhysicalStreamingWindow::PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                                 idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

class StreamingWindowGlobalState : public GlobalOperatorState {
public:
	StreamingWindowGlobalState() : row_number(1) {
	}

	//! The next row number.
	std::atomic<int64_t> row_number;
};

class StreamingWindowState : public OperatorState {
public:
	StreamingWindowState() : initialized(false) {
	}

	void Initialize(Allocator &allocator, DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_FIRST_VALUE: {
				auto &wexpr = (BoundWindowExpression &)expr;

				// Just execute the expression once
				ExpressionExecutor executor(allocator);
				executor.AddExpression(*wexpr.children[0]);
				DataChunk result;
				result.Initialize(allocator, {wexpr.children[0]->return_type});
				executor.Execute(input, result);

				const_vectors.push_back(make_unique<Vector>(result.GetValue(0, 0)));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors.push_back(make_unique<Vector>(Value((double)0)));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors.push_back(make_unique<Vector>(Value((int64_t)1)));
				break;
			}
			default:
				const_vectors.push_back(nullptr);
			}
		}
		initialized = true;
	}

public:
	bool initialized;
	vector<unique_ptr<Vector>> const_vectors;
};

unique_ptr<GlobalOperatorState> PhysicalStreamingWindow::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<StreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalStreamingWindow::GetOperatorState(ExecutionContext &context) const {
	return make_unique<StreamingWindowState>();
}

OperatorResultType PhysicalStreamingWindow::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = (StreamingWindowGlobalState &)gstate_p;
	auto &state = (StreamingWindowState &)state_p;
	if (!state.initialized) {
		auto &allocator = Allocator::Get(context.client);
		state.Initialize(allocator, input, select_list);
	}
	// Put payload columns in place
	for (idx_t col_idx = 0; col_idx < input.data.size(); col_idx++) {
		chunk.data[col_idx].Reference(input.data[col_idx]);
	}
	// Compute window function
	const idx_t count = input.size();
	for (idx_t expr_idx = 0; expr_idx < select_list.size(); expr_idx++) {
		idx_t col_idx = input.data.size() + expr_idx;
		auto &expr = *select_list[expr_idx];
		switch (expr.GetExpressionType()) {
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_PERCENT_RANK:
		case ExpressionType::WINDOW_RANK:
		case ExpressionType::WINDOW_RANK_DENSE: {
			// Reference constant vector
			chunk.data[col_idx].Reference(*state.const_vectors[expr_idx]);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			// Set row numbers
			auto rdata = FlatVector::GetData<int64_t>(chunk.data[col_idx]);
			for (idx_t i = 0; i < count; i++) {
				rdata[i] = gstate.row_number + i;
			}
			break;
		}
		default:
			throw NotImplementedException("%s for StreamingWindow", ExpressionTypeToString(expr.GetExpressionType()));
		}
	}
	gstate.row_number += count;
	chunk.SetCardinality(count);
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalStreamingWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb
