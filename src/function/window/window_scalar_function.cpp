#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/window/window_executor.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

namespace {

struct ScalarWindowBindData : public FunctionData {
	using BindInfoPtr = unique_ptr<FunctionData>;

	ScalarWindowBindData(ClientContext &client, BoundWindowExpression &wexpr) : client(client), wexpr(wexpr.Copy()) {
	}

	ScalarWindowBindData(const ScalarWindowBindData &other)
	    : FunctionData(other), client(other.client), wexpr(other.wexpr->Copy()) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ScalarWindowBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ScalarWindowBindData>();
		if (!wexpr->Equals(*other.wexpr)) {
			return false;
		}
		return true;
	}

	ClientContext &client;
	unique_ptr<Expression> wexpr;
};

//	A degenerate frame holds at most the current row, which these exclusions remove
idx_t DegenerateFrameWidth(const BoundWindowExpression &wexpr) {
	switch (wexpr.WindowExclude()) {
	case WindowExcludeMode::CURRENT_ROW:
	case WindowExcludeMode::GROUP:
		return 0;
	default:
		return 1;
	}
}

void AggregateScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &scalar_info = func_expr.BindInfo()->Cast<ScalarWindowBindData>();
	auto &wexpr = scalar_info.wexpr->Cast<BoundWindowExpression>();
	auto bind_info = wexpr.BindInfo().get();

	//	Is the frame empty?
	const idx_t width = DegenerateFrameWidth(wexpr);

	auto &client = scalar_info.client;
	ThreadContext thread(client);
	ExecutionContext context(client, thread, nullptr);
	auto &allocator = Allocator::Get(client);
	ArenaAllocator arena_allocator(allocator);
	const auto count = args.size();

	//	Aggregate each row separately
	AggregateObject aggr(wexpr);
	AggregateStateInput agg_input(aggr.function, bind_info);

	auto callbacks = aggr.function.GetCallbacks();
	const idx_t state_size = callbacks.GetStateSizeCallback()(agg_input);
	vector<data_t> agg_state(state_size * count);

	//	Allocate the states
	Vector statev(LogicalType::POINTER);
	auto states = FlatVector::GetDataMutable<data_ptr_t>(statev);
	auto state_ptr = agg_state.data();
	for (idx_t i = 0; i < count; ++i) {
		states[i] = state_ptr;
		state_ptr += state_size;
	}

	//	Initialise the states
	auto initialize = callbacks.GetStateInitCallback();
	initialize(agg_input, states, count);

	//	Update the state if the frame is not empty
	AggregateFinalizeInputData aggr_bind_info(aggr.function, bind_info, arena_allocator);
	if (width) {
		//	The arguments reference the input row
		DataChunk inputs;
		ExpressionExecutor input_exec(client);
		vector<LogicalType> input_types;
		for (const auto &child : wexpr.GetChildren()) {
			input_exec.AddExpression(*child);
			input_types.emplace_back(child->GetReturnType());
		}
		if (!input_types.empty()) {
			inputs.Initialize(allocator, input_types);
			input_exec.Execute(args, inputs);
			inputs.CheckCardinality(count);
		} else {
			inputs.SetCardinalityUnsafe(count);
		}

		//	Rows rejected by the filter leave their state empty
		auto update = aggr.function.GetCallbacks().GetStateUpdateCallback();
		if (wexpr.Filter()) {
			SelectionVector sel(count);
			ExpressionExecutor filter_exec(client, *wexpr.Filter());
			const auto update_count = filter_exec.SelectExpression(args, sel);
			inputs.Slice(sel, update_count);
			Vector update_states(statev, sel, update_count);
			update(inputs.data.data(), aggr_bind_info, inputs.ColumnCount(), update_states, update_count);
		} else {
			update(inputs.data.data(), aggr_bind_info, inputs.ColumnCount(), statev, count);
		}
	}

	//	Finalize the states
	auto finalize = aggr.function.GetCallbacks().GetStateFinalizeCallback();
	finalize(statev, aggr_bind_info, result, count, 0);

	//	Deallocate the states
	auto destructor = callbacks.GetStateDestructorCallback();
	if (destructor) {
		destructor(statev, aggr_bind_info, count);
	}
}

void WindowScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &scalar_info = func_expr.BindInfo()->Cast<ScalarWindowBindData>();
	auto &wexpr = scalar_info.wexpr->Cast<BoundWindowExpression>();

	//	Is the frame empty?
	const idx_t width = DegenerateFrameWidth(wexpr);

	auto &client = scalar_info.client;
	ThreadContext thread(client);
	ExecutionContext context(client, thread, nullptr);
	auto &allocator = Allocator::Get(client);
	ArenaAllocator arena_allocator(allocator);
	const auto count = args.size();
	if (width) {
		//	Build shared expressions
		WindowSharedExpressions shared;
		WindowExecutor wexec(wexpr, shared);

		DataChunk coll_chunk;
		ExpressionExecutor coll_exec(client);
		shared.PrepareCollection(coll_exec, coll_chunk);

		//	Build acceleration data
		//	One partition in which every row is its own peer group, so the frame is the current row
		ValidityMask partition_mask;
		partition_mask.Initialize(count);
		partition_mask.SetAllInvalid(count);
		partition_mask.SetValid(0);
		ValidityMask order_mask;
		order_mask.Initialize(count);
		auto gsink = wexec.GetGlobalState(client, count, partition_mask, order_mask);
		auto lsink = wexec.GetLocalState(context, *gsink);

		//	Compute fully materialised expressions
		auto &buffer_manager = BufferManager::GetBufferManager(client);
		auto collection = make_uniq<WindowCollection>(buffer_manager, count, coll_chunk.GetTypes());
		if (coll_chunk.data.empty()) {
			coll_chunk.SetChildCardinality(count);
		} else {
			coll_exec.Execute(args, coll_chunk);
			auto builder = make_uniq<WindowBuilder>(*collection);
			builder->Sink(coll_chunk, 0);
		}

		// Compute sink expressions
		DataChunk sink_chunk;
		ExpressionExecutor sink_exec(client);
		shared.PrepareSink(sink_exec, sink_chunk);
		if (sink_chunk.data.empty()) {
			sink_chunk.SetChildCardinality(count);
		} else {
			sink_exec.Execute(args, sink_chunk);
		}

		InterruptState interrupt;
		OperatorSinkInput sink {*gsink, *lsink, interrupt};
		wexec.Sink(context, sink_chunk, coll_chunk, 0, sink);

		collection->Combine(shared.coll_validity);
		wexec.Finalize(context, collection, sink);

		//	Evaluate
		DataChunk eval_chunk;
		ExpressionExecutor eval_exec(client);
		shared.PrepareEvaluate(eval_exec, eval_chunk);
		if (eval_chunk.data.empty()) {
			eval_chunk.SetChildCardinality(count);
		} else {
			eval_exec.Execute(args, eval_chunk);
		}

		wexec.Evaluate(context, 0, eval_chunk, result, sink, count);
	} else {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

} // namespace

unique_ptr<Expression> FunctionBinder::BindScalarWindowFunction(BoundWindowExpression &wexpr,
                                                                const vector<LogicalType> &input_types) {
	//	The window expressions reference the input row, so the function receives all of it
	vector<unique_ptr<Expression>> children;
	for (idx_t col_idx = 0; col_idx < input_types.size(); ++col_idx) {
		children.emplace_back(make_uniq<BoundReferenceExpression>(input_types[col_idx], col_idx));
	}
	auto &aggr = wexpr.AggregateFunction();
	auto func = aggr ? AggregateScalarFunc : WindowScalarFunc;
	ScalarFunction scalar(wexpr.GetName(), input_types, wexpr.GetReturnType(), func);
	if (aggr) {
		scalar.SetProperties(aggr->GetProperties());
	} else {
		scalar.SetProperties(wexpr.WindowFunction()->GetProperties());
	}
	auto bind_info = make_uniq<ScalarWindowBindData>(context, wexpr);
	BoundScalarFunction bound(scalar);
	return make_uniq<BoundFunctionExpression>(bound, std::move(children), std::move(bind_info));
}

} // namespace duckdb
