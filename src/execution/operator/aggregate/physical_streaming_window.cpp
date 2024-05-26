#include "duckdb/execution/operator/aggregate/physical_streaming_window.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

bool PhysicalStreamingWindow::IsStreamingFunction(unique_ptr<Expression> &expr) {
	auto &wexpr = expr->Cast<BoundWindowExpression>();
	if (!wexpr.partitions.empty() || !wexpr.orders.empty() || wexpr.ignore_nulls ||
	    wexpr.exclude_clause != WindowExcludeMode::NO_OTHER) {
		return false;
	}
	switch (wexpr.type) {
	// TODO: add more expression types here?
	case ExpressionType::WINDOW_AGGREGATE:
		// We can stream aggregates if they are "running totals"
		// TODO: Support DISTINCT
		return wexpr.start == WindowBoundary::UNBOUNDED_PRECEDING && wexpr.end == WindowBoundary::CURRENT_ROW_ROWS &&
		       !wexpr.distinct;
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_ROW_NUMBER:
		return true;
	default:
		return false;
	}
}

PhysicalStreamingWindow::PhysicalStreamingWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                                                 idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, std::move(types), estimated_cardinality), select_list(std::move(select_list)) {
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

	struct AggregateState {
		explicit AggregateState(BoundWindowExpression &wexpr)
		    : arena_allocator(Allocator::DefaultAllocator()), statev(LogicalType::POINTER, data_ptr_cast(&state_ptr)) {
			D_ASSERT(wexpr.GetExpressionType() == ExpressionType::WINDOW_AGGREGATE);
			auto &aggregate = *wexpr.aggregate;
			bind_data = wexpr.bind_info.get();
			dtor = aggregate.destructor;
			state.resize(aggregate.state_size());
			state_ptr = state.data();
			aggregate.initialize(state.data());
			for (auto &child : wexpr.children) {
				arg_types.push_back(child->return_type);
			}
			if (wexpr.filter_expr) {
				filter_sel.Initialize();
			}
		}

		~AggregateState() {
			if (dtor) {
				AggregateInputData aggr_input_data(bind_data, arena_allocator);
				state_ptr = state.data();
				dtor(statev, aggr_input_data, 1);
			}
		}

		ArenaAllocator arena_allocator;
		StateBuffer state;
		data_ptr_t state_ptr = nullptr;
		Vector statev;
		FunctionData *bind_data = nullptr;
		aggregate_destructor_t dtor = nullptr;
		SelectionVector filter_sel;
		int64_t unfiltered = 0;
		vector<LogicalType> arg_types;
	};

	explicit StreamingWindowState(ClientContext &client) : initialized(false), allocator(Allocator::Get(client)) {
	}

	~StreamingWindowState() override {
	}

	void Initialize(ClientContext &context, DataChunk &input, const vector<unique_ptr<Expression>> &expressions) {
		const_vectors.resize(expressions.size());
		aggregate_states.resize(expressions.size());

		for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
			auto &expr = *expressions[expr_idx];
			auto &wexpr = expr.Cast<BoundWindowExpression>();
			switch (expr.GetExpressionType()) {
			case ExpressionType::WINDOW_AGGREGATE:
				aggregate_states[expr_idx] = make_uniq<AggregateState>(wexpr);
				break;
			case ExpressionType::WINDOW_FIRST_VALUE: {
				// Just execute the expression once
				ExpressionExecutor executor(context);
				executor.AddExpression(*wexpr.children[0]);
				DataChunk result;
				result.Initialize(Allocator::Get(context), {wexpr.children[0]->return_type});
				executor.Execute(input, result);

				const_vectors[expr_idx] = make_uniq<Vector>(result.GetValue(0, 0));
				break;
			}
			case ExpressionType::WINDOW_PERCENT_RANK: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((double)0));
				break;
			}
			case ExpressionType::WINDOW_RANK:
			case ExpressionType::WINDOW_RANK_DENSE: {
				const_vectors[expr_idx] = make_uniq<Vector>(Value((int64_t)1));
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
	vector<unique_ptr<AggregateState>> aggregate_states;
	Allocator &allocator;
};

unique_ptr<GlobalOperatorState> PhysicalStreamingWindow::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<StreamingWindowGlobalState>();
}

unique_ptr<OperatorState> PhysicalStreamingWindow::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<StreamingWindowState>(context.client);
}

OperatorResultType PhysicalStreamingWindow::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<StreamingWindowGlobalState>();
	auto &state = state_p.Cast<StreamingWindowState>();

	if (!state.initialized) {
		state.Initialize(context.client, input, select_list);
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
			auto &wexpr = expr.Cast<BoundWindowExpression>();
			auto &aggregate = *wexpr.aggregate;
			auto &aggr_state = *state.aggregate_states[expr_idx];
			auto &statev = aggr_state.statev;
			AggregateInputData aggr_input_data(wexpr.bind_info.get(), aggr_state.arena_allocator);

			// Compute the FILTER mask (if any)
			ValidityMask filter_mask;
			if (wexpr.filter_expr) {
				auto &filter_sel = aggr_state.filter_sel;
				ExpressionExecutor filter_executor(context.client, *wexpr.filter_expr);
				const auto filtered = filter_executor.SelectExpression(input, filter_sel);
				if (filtered < count) {
					filter_mask.Initialize(count);
					filter_mask.SetAllInvalid(count);
					for (idx_t f = 0; f < filtered; ++f) {
						filter_mask.SetValid(filter_sel.get_index(f));
					}
				}
			}

			// Check for COUNT(*)
			if (wexpr.children.empty()) {
				D_ASSERT(GetTypeIdSize(result.GetType().InternalType()) == sizeof(int64_t));
				auto data = FlatVector::GetData<int64_t>(result);
				auto &unfiltered = aggr_state.unfiltered;
				for (idx_t i = 0; i < count; ++i) {
					unfiltered += int64_t(filter_mask.RowIsValid(i));
					data[i] = unfiltered;
				}
				break;
			}

			// Compute the arguments
			ExpressionExecutor executor(context.client);
			for (auto &child : wexpr.children) {
				executor.AddExpression(*child);
			}

			DataChunk arg_chunk;
			arg_chunk.Initialize(state.allocator, aggr_state.arg_types);
			executor.Execute(input, arg_chunk);

			// Iterate through them using a single SV
			arg_chunk.Flatten();
			sel_t s = 0;
			SelectionVector sel(&s);
			DataChunk row;
			row.Initialize(state.allocator, aggr_state.arg_types);
			row.Slice(sel, 1);
			// This doesn't work for STRUCTs because the SV
			// is not copied to the children when you slice
			vector<column_t> structs;
			for (column_t col_idx = 0; col_idx < arg_chunk.ColumnCount(); ++col_idx) {
				auto &col_vec = row.data[col_idx];
				DictionaryVector::Child(col_vec).Reference(arg_chunk.data[col_idx]);
				if (col_vec.GetType().InternalType() == PhysicalType::STRUCT) {
					structs.emplace_back(col_idx);
				}
			}

			// Update the state and finalize it one row at a time.
			for (idx_t i = 0; i < count; ++i) {
				sel.set_index(0, i);
				for (const auto struct_idx : structs) {
					row.data[struct_idx].Slice(arg_chunk.data[struct_idx], sel, 1);
				}
				if (filter_mask.RowIsValid(i)) {
					aggregate.update(row.data.data(), aggr_input_data, row.ColumnCount(), statev, 1);
				}
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
			int64_t start_row = gstate.row_number;
			auto rdata = FlatVector::GetData<int64_t>(chunk.data[col_idx]);
			for (idx_t i = 0; i < count; i++) {
				rdata[i] = NumericCast<int64_t>(start_row + NumericCast<int64_t>(i));
			}
			break;
		}
		default:
			throw NotImplementedException("%s for StreamingWindow", ExpressionTypeToString(expr.GetExpressionType()));
		}
	}
	gstate.row_number += NumericCast<int64_t>(count);
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
