#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window_function.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
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

	ScalarWindowBindData(ClientContext &client, BoundWindowExpression &wexpr) : client(client), wexpr(wexpr) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ScalarWindowBindData>(*this);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ScalarWindowBindData>();
		if (!wexpr.Equals(other.wexpr)) {
			return false;
		}
		return true;
	}

	ClientContext &client;
	BoundWindowExpression &wexpr;
};

void AggregateScalarFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &scalar_info = func_expr.BindInfo()->Cast<ScalarWindowBindData>();
	auto &wexpr = scalar_info.wexpr;
	auto bind_info = wexpr.BindInfo().get();

	//	Is the frame empty?
	auto &frame_begin = wexpr.StartExpr()->Cast<BoundConstantExpression>();
	auto &frame_end = wexpr.EndExpr()->Cast<BoundConstantExpression>();
	const idx_t width = idx_t(frame_begin.GetValue() != frame_end.GetValue());

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
	state_ptr = agg_state.data();
	initialize(agg_input, &state_ptr, count);

	//	Update the state if the frame is not empty
	AggregateFinalizeInputData aggr_bind_info(aggr.function, bind_info, arena_allocator);
	if (width) {
		auto update = aggr.function.GetCallbacks().GetStateUpdateCallback();
		update(args.data.data(), aggr_bind_info, args.data.size(), statev, count);
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
	auto &wexpr = scalar_info.wexpr;

	//	Is the frame empty?
	auto &frame_begin = wexpr.StartExpr()->Cast<BoundConstantExpression>();
	auto &frame_end = wexpr.EndExpr()->Cast<BoundConstantExpression>();
	const idx_t width = idx_t(frame_begin.GetValue() != frame_end.GetValue());

	auto &client = scalar_info.client;
	ThreadContext thread(client);
	ExecutionContext context(client, thread, nullptr);
	auto &allocator = Allocator::Get(client);
	ArenaAllocator arena_allocator(allocator);
	const auto count = args.size();
	if (width) {
		//	Set the bounds to the single frame.
		vector<LogicalType> bounds_types(8, LogicalType(LogicalTypeId::UBIGINT));
		DataChunk bounds;
		bounds.Initialize(allocator, bounds_types);
		auto frame_begin = FlatVector::Writer<idx_t>(bounds.data[PARTITION_BEGIN], count);
		auto frame_end = FlatVector::Writer<idx_t>(bounds.data[PARTITION_END], count);
		for (idx_t i = 0; i < count; ++i) {
			frame_begin.WriteValue(i);
			frame_end.WriteValue(i + 1);
		}
		for (column_t col = 2; col < bounds.ColumnCount(); ++col) {
			bounds.data[col].Reference(bounds.data[col % 2]);
		}

		//	Build shared expressions
		WindowSharedExpressions shared;
		WindowExecutor wexec(wexpr, shared);

		DataChunk sink_chunk;
		ExpressionExecutor sink_exec(client);
		shared.PrepareSink(sink_exec, sink_chunk);
		sink_exec.Execute(args, sink_chunk);

		DataChunk coll_chunk;
		ExpressionExecutor coll_exec(client);
		shared.PrepareCollection(coll_exec, coll_chunk);

		optional_ptr<WindowCollection> collection;
		if (coll_chunk.data.empty()) {
			coll_chunk.SetChildCardinality(args.size());
		} else {
			coll_exec.Execute(args, coll_chunk);
			auto &buffer_manager = BufferManager::GetBufferManager(client);
			collection = make_uniq<WindowCollection>(buffer_manager, count, coll_chunk.GetTypes());
			auto builder = make_uniq<WindowBuilder>(*collection);
			builder->Sink(coll_chunk, 0);
		}

		DataChunk eval_chunk;
		ExpressionExecutor eval_exec(client);
		shared.PrepareEvaluate(eval_exec, eval_chunk);
		eval_exec.Execute(args, eval_chunk);

		//	Build acceleration data
		ValidityMask mask(count);
		mask.SetAllInvalid(count);
		auto gsink = wexec.GetGlobalState(client, count, mask, mask);
		auto lsink = wexec.GetLocalState(context, *gsink);

		InterruptState interrupt;
		OperatorSinkInput sink {*gsink, *lsink, interrupt};
		wexec.Sink(context, sink_chunk, coll_chunk, 0, sink);

		wexec.Finalize(context, collection, sink);
		wexec.Evaluate(context, 0, eval_chunk, result, sink, count);
	} else {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
	}
}

} // namespace

unique_ptr<Expression> FunctionBinder::BindScalarWindowFunction(BoundWindowExpression &wexpr) {
	vector<LogicalType> arguments;
	for (const auto &child : wexpr.GetChildren()) {
		arguments.emplace_back(child->GetReturnType());
	}
	auto func = wexpr.AggregateFunction() ? AggregateScalarFunc : WindowScalarFunc;
	ScalarFunction scalar(wexpr.GetName(), arguments, wexpr.GetReturnType(), func);
	auto bind_info = make_uniq<ScalarWindowBindData>(context, wexpr);
	BoundScalarFunction bound(scalar);

	return make_uniq<BoundFunctionExpression>(bound, std::move(wexpr.GetChildrenMutable()), std::move(bind_info));
}

} // namespace duckdb
