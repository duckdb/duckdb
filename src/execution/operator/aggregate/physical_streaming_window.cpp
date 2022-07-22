#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
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
	using StateBuffer = vector<data_t>;

	StreamingWindowState() : initialized(false), statev(LogicalType::POINTER, (data_ptr_t)&state_ptr) {
	}

	~StreamingWindowState() override {
		for (size_t i = 0; i < aggregate_dtors.size(); ++i) {
			auto dtor = aggregate_dtors[i];
			if (dtor) {
				state_ptr = aggregate_states[i].data();
				dtor(statev, 1);
			}
		}
	}

	void Initialize(Allocator &allocator, DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		const_vectors.resize(expressions.size());
		aggregate_states.resize(expressions.size());
		aggregate_dtors.resize(expressions.size(), nullptr);

		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			auto &wexpr = (BoundWindowExpression &)expr;
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_AGGREGATE: {
				auto &aggregate = *wexpr.aggregate;
				auto &state = aggregate_states[expr_idx];
				aggregate_dtors[expr_idx] = aggregate.destructor;
				state.resize(aggregate.state_size());
				aggregate.initialize(state.data());
				break;
			}
			case ExpressionType::WINDOW_FIRST_VALUE: {
				// Just execute the expression once
				ExpressionExecutor executor(allocator);
				executor.AddExpression(*wexpr.children[0]);
				DataChunk result;
				result.Initialize(allocator, {wexpr.children[0]->return_type});
				executor.Execute(input, result);

				const_vectors[expr_idx] = make_unique<Vector>(result.GetValue(0, 0));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors[expr_idx] = make_unique<Vector>(Value((double)0));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors[expr_idx] = make_unique<Vector>(Value((int64_t)1));
				break;
			}
			default:
				break;
			}
		}
		initialized = true;
	}

public:
	bool initialized;
	vector<unique_ptr<Vector>> const_vectors;

	// Aggregation
	vector<StateBuffer> aggregate_states;
	vector<aggregate_destructor_t> aggregate_dtors;
	data_ptr_t state_ptr;
	Vector statev;
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
		auto &result = chunk.data[col_idx];
		switch (expr.GetExpressionType()) {
		case ExpressionType::WINDOW_AGGREGATE: {
			//	Establish the aggregation environment
			auto &wexpr = (BoundWindowExpression &)expr;
			auto &aggregate = *wexpr.aggregate;
			auto &statev = state.statev;
			state.state_ptr = state.aggregate_states[expr_idx].data();
			AggregateInputData aggr_input_data(wexpr.bind_info.get());

			// Check for COUNT(*)
			if (wexpr.children.empty()) {
				D_ASSERT(GetTypeIdSize(result.GetType().InternalType()) == sizeof(int64_t));
				auto data = FlatVector::GetData<int64_t>(result);
				for (idx_t i = 0; i < input.size(); ++i) {
					data[i] = gstate.row_number + i;
				}
				break;
			}

			// Compute the arguments
			auto &allocator = Allocator::Get(context.client);
			ExpressionExecutor executor(allocator);
			vector<LogicalType> payload_types;
			for (auto &child : wexpr.children) {
				payload_types.push_back(child->return_type);
				executor.AddExpression(*child);
			}

			DataChunk payload;
			payload.Initialize(executor.allocator, payload_types);
			executor.Execute(input, payload);

			// Iterate through them using a single SV
			payload.Flatten();
			DataChunk row;
			row.Initialize(allocator, payload_types);
			sel_t s = 0;
			SelectionVector sel(&s);
			row.Slice(sel, 1);
			for (size_t col_idx = 0; col_idx < payload.ColumnCount(); ++col_idx) {
				DictionaryVector::Child(row.data[col_idx]).Reference(payload.data[col_idx]);
			}

			// Update the state and finalize it one row at a time.
			for (idx_t i = 0; i < input.size(); ++i) {
				sel.set_index(0, i);
				aggregate.update(row.data.data(), aggr_input_data, row.ColumnCount(), statev, 1);
				aggregate.finalize(statev, aggr_input_data, result, 1, i);
			}
			break;
		}
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
