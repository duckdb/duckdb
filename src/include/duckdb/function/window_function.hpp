//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

class BoundWindowExpression;
struct WindowSharedExpressions;
class WindowExecutor;
class GlobalSinkState;
class LocalSinkState;
class WindowCollection;

//	Column indexes of the bounds chunk
enum WindowBounds : uint8_t {
	PARTITION_BEGIN,
	PARTITION_END,
	PEER_BEGIN,
	PEER_END,
	VALID_BEGIN,
	VALID_END,
	FRAME_BEGIN,
	FRAME_END
};

// C++ 11 won't do this automatically...
struct WindowBoundsHash {
	inline uint64_t operator()(const WindowBounds &value) const {
		return value;
	}
};

using WindowBoundsSet = unordered_set<WindowBounds, WindowBoundsHash>;

struct WindowFunctionInfo {
	DUCKDB_API virtual ~WindowFunctionInfo();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class BindWindowFunctionInput {
public:
	using OptionalOrdering = optional_ptr<vector<OrderByNode>>;

	BindWindowFunctionInput(ClientContext &context_p, WindowFunction &bound_function_p,
	                        vector<unique_ptr<Expression>> &arguments_p, OptionalOrdering orders_p = nullptr,
	                        OptionalOrdering arg_orders_p = nullptr)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p), orders(orders_p),
	      arg_orders(arg_orders_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	WindowFunction &GetBoundFunction() const {
		return bound_function;
	}
	vector<unique_ptr<Expression>> &GetArguments() const {
		return arguments;
	}
	bool HasOrders() const {
		return orders.get();
	}
	const vector<OrderByNode> &GetOrders() const {
		return *orders;
	}
	bool HasArgumentOrders() const {
		return arg_orders.get();
	}
	const vector<OrderByNode> &GetArgumentOrders() const {
		return *arg_orders;
	}

private:
	ClientContext &context;
	WindowFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
	OptionalOrdering orders;
	OptionalOrdering arg_orders;
};

//! Binds the window function and creates the function data
typedef unique_ptr<FunctionData> (*window_bind_function_t)(BindWindowFunctionInput &input);

//! Requests framing bounds that the function uses
typedef void (*window_bounds_function_t)(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr);

//! Requests expression sharing. If not provided, all children will be registered for evaluate time.
typedef void (*window_sharing_function_t)(WindowExecutor &executor, WindowSharedExpressions &sharing);

//! Constructs a global state for the hash group.
//! If not provided, a default WindowExecutorGlobalState will be generated, with references to the parameters
typedef unique_ptr<GlobalSinkState> (*window_global_function_t)(ClientContext &client, const WindowExecutor &executor,
                                                                const idx_t payload_count,
                                                                const ValidityMask &partition_mask,
                                                                const ValidityMask &order_mask);

//! Constructs a thread local state for the hash group.
//! If not provided, a default WindowExecutorLocalState will be generated, with references to the parameters
typedef unique_ptr<LocalSinkState> (*window_local_function_t)(ExecutionContext &context, const GlobalSinkState &gstate);

//! Sinks data into the thread-local state
typedef void (*window_sink_function_t)(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                       idx_t input_idx, OperatorSinkInput &sink);

//! Finalizes the thread-local state (builds all data structures needed for
typedef void (*window_finalize_function_t)(ExecutionContext &context, optional_ptr<WindowCollection> collection,
                                           OperatorSinkInput &sink);

//! Computes the function for a single chunk
typedef void (*window_evaluate_function_t)(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds,
                                           Vector &result, idx_t row_idx, OperatorSinkInput &sink);

//! Does the function support streaming?
typedef bool (*window_canstream_function_t)(ClientContext &client, const BoundWindowExpression &wexpr, idx_t max_delta);

//! Constructs a thread local state for the streaming function
typedef unique_ptr<LocalSourceState> (*window_streaming_state_function_t)(ClientContext &client, DataChunk &input,
                                                                          const BoundWindowExpression &wexpr);

//! Evaluates the next chunk of the streaming function
typedef void (*window_stream_function_t)(ExecutionContext &context, DataChunk &input, DataChunk &delayed,
                                         idx_t delayed_capacity, Vector &result, LocalSourceState &lstate);

//! Serialization of the binding data (if any)
typedef void (*window_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                   const WindowFunction &function);
typedef unique_ptr<FunctionData> (*window_deserialize_t)(Deserializer &deserializer, WindowFunction &function);

class WindowFunction : public BaseScalarFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr,
	               window_bounds_function_t bounds = nullptr, window_sharing_function_t sharing = nullptr,
	               window_global_function_t global = nullptr, window_local_function_t local = nullptr,
	               window_sink_function_t sink = nullptr, window_finalize_function_t finalize = nullptr,
	               window_evaluate_function_t evaluate = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind), bounds(bounds), sharing(sharing), global(global), local(local),
	      sink(sink), finalize(finalize), evaluate(evaluate) {
	}

	WindowFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, ExpressionType window_enum,
	               window_bind_function_t bind = nullptr, window_bounds_function_t bounds = nullptr,
	               window_sharing_function_t sharing = nullptr, window_global_function_t global = nullptr,
	               window_local_function_t local = nullptr, window_sink_function_t sink = nullptr,
	               window_finalize_function_t finalize = nullptr, window_evaluate_function_t evaluate = nullptr)
	    : WindowFunction(string(), arguments, return_type, window_enum, bind, bounds, sharing, global, local, sink,
	                     finalize, evaluate) {
	}

	// clang-format off
	bool HasBindCallback() const { return bind != nullptr; }
	window_bind_function_t GetBindCallback() const { return bind; }
	void SetBindCallback(window_bind_function_t callback) { bind = callback; }
	unique_ptr<FunctionData> Bind(BindWindowFunctionInput &bind_input) { return GetBindCallback()(bind_input); }
	unique_ptr<FunctionData> Bind(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
		BindWindowFunctionInput bind_input(context, *this, arguments);
		return Bind(bind_input);
	}

	bool HasBoundsCallback() const { return bounds != nullptr; }
	window_bounds_function_t GetBoundsCallback() const { return bounds; }
	void SetBoundsCallback(window_bounds_function_t callback) { bounds = callback; }
	void GetBounds(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr) const {
		D_ASSERT(HasBoundsCallback());
		GetBoundsCallback()(bounds, wexpr);
	}

	bool HasSharingCallback() const { return sharing != nullptr; }
	window_sharing_function_t GetSharingCallback() const { return sharing; }
	void SetSharingCallback(window_sharing_function_t callback) { sharing = callback; }
	void GetSharing(WindowExecutor &executor, WindowSharedExpressions &sharing) const {
		D_ASSERT(HasSharingCallback());
		GetSharingCallback()(executor, sharing);
	}

	bool HasGlobalCallback() const { return global != nullptr; }
	window_global_function_t GetGlobalCallback() const { return global; }
	void SetGlobalCallback(window_global_function_t callback) { global = callback; }
	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const WindowExecutor &executor,
                                                                const idx_t payload_count,
                                                                const ValidityMask &partition_mask,
                                                                const ValidityMask &order_mask) const {
		D_ASSERT(HasGlobalCallback());
		return GetGlobalCallback()(client, executor, payload_count, partition_mask, order_mask);
	}

	bool HasLocalCallback() const { return local != nullptr; }
	window_local_function_t GetLocalCallback() const { return local; }
	void SetLocalCallback(window_local_function_t callback) { local = callback; }
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const {
		D_ASSERT(HasLocalCallback());
		return GetLocalCallback()(context, gstate);
	}

	bool HasSinkCallback() const { return sink != nullptr; }
	window_sink_function_t GetSinkCallback() const { return sink; }
	void SetSinkCallback(window_sink_function_t callback) { sink = callback; }
	void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                    idx_t input_idx, OperatorSinkInput &sink) const {
		D_ASSERT(HasSinkCallback());
		return GetSinkCallback()(context, sink_chunk, coll_chunk, input_idx, sink);
	}

	bool HasFinalizeCallback() const { return finalize != nullptr; }
	window_finalize_function_t GetFinalizeCallback() const { return finalize; }
	void SetFinalizeCallback(window_finalize_function_t callback) { finalize = callback; }
	void Finalize(ExecutionContext &context, optional_ptr<WindowCollection> collection, OperatorSinkInput &sink) const {
		D_ASSERT(HasFinalizeCallback());
		return GetFinalizeCallback()(context, collection, sink);
	}

	bool HasEvaluateCallback() const { return evaluate != nullptr; }
	window_evaluate_function_t GetEvaluateCallback() const { return evaluate; }
	void SetEvaluateCallback(window_evaluate_function_t callback) { evaluate = callback; }
	void Evaluate(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result, idx_t row_idx,
				  OperatorSinkInput &sink) const {
		D_ASSERT(HasEvaluateCallback());
		return GetEvaluateCallback()(context, eval_chunk, bounds, result, row_idx, sink);
	}

	bool HasCanStreamCallback() const { return can_stream != nullptr; }
	window_canstream_function_t GetCanStreamCallback() const { return can_stream; }
	void SetCanStreamCallback(window_canstream_function_t callback) { can_stream = callback; }
	bool CanStream(ClientContext &client, const BoundWindowExpression &wexpr, idx_t max_delta) const {
		D_ASSERT(HasCanStreamCallback());
		return GetCanStreamCallback()(client, wexpr, max_delta);
	}

	bool HasStreamingStateCallback() const { return streaming_state != nullptr; }
	window_streaming_state_function_t GetStreamingStateCallback() const { return streaming_state; }
	void SetStreamingStateCallback(window_streaming_state_function_t callback) { streaming_state = callback; }
	unique_ptr<LocalSourceState> GetStreamingState(ClientContext &client, DataChunk &input,
												   const BoundWindowExpression &wexpr) const {
		D_ASSERT(HasStreamingStateCallback());
		return GetStreamingStateCallback()(client, input, wexpr);
	}

	bool HasStreamingDataCallback() const { return stream != nullptr; }
	window_stream_function_t GetStreamingDataCallback() const { return stream; }
	void SetStreamingDataCallback(window_stream_function_t callback) { stream = callback; }
	void GetStreamingData(ExecutionContext &context, DataChunk &input, DataChunk &delayed, idx_t delayed_capacity,
	 					  Vector &result, LocalSourceState &lstate) const {
		D_ASSERT(HasStreamingDataCallback());
		GetStreamingDataCallback()(context, input, delayed, delayed_capacity, result, lstate);
	}

	bool HasSerializationCallbacks() const { return false; }
	void SetSerializeCallback(window_serialize_t callback) { serialize = callback; }
	void SetDeserializeCallback(window_deserialize_t callback) { deserialize = callback; }
	window_serialize_t GetSerializeCallback() const { return serialize; }
	window_deserialize_t GetDeserializeCallback() const { return deserialize; }
	// clang-format on

	//! The expression enum for the window function
	const ExpressionType window_enum;

	//! Does the window function support DISTINCT?
	bool can_distinct = false;
	//! Does the window function support FILTER?
	bool can_filter = false;
	//! Does the window function support ORDER BY arguments?
	bool can_order_by = true;
	//! Does the window function support EXCLUDE?
	bool can_exclude = false;
	//! Does the window function support RESPECT/IGNORE NULLS?
	bool can_ignore_nulls = true;

	//! The bind function (may be null)
	window_bind_function_t bind = nullptr;

	//! The framing bounds lists
	window_bounds_function_t bounds = nullptr;
	//! The children sharing requirements
	window_sharing_function_t sharing = nullptr;
	//! The global state constructor
	window_global_function_t global = nullptr;
	//! The local state constructor
	window_local_function_t local = nullptr;
	//! The local state data sink
	window_sink_function_t sink = nullptr;
	//! The local state finalize operation
	window_finalize_function_t finalize = nullptr;
	//! The thread-local evaluation function
	window_evaluate_function_t evaluate = nullptr;

	//! Can the function stream?
	window_canstream_function_t can_stream = nullptr;
	//! Get the streaming state object
	window_streaming_state_function_t streaming_state = nullptr;
	//! The streaming evaluation function
	window_stream_function_t stream = nullptr;

	//! Serialization specialization. Not yet implemented
	window_serialize_t serialize = nullptr;
	window_deserialize_t deserialize = nullptr;

public:
	//! Additional function info, passed to the bind
	shared_ptr<WindowFunctionInfo> function_info;

public:
	bool operator==(const WindowFunction &rhs) const {
		return name == rhs.name;
	}
	bool operator!=(const WindowFunction &rhs) const {
		return !(*this == rhs);
	}
};

} // namespace duckdb
