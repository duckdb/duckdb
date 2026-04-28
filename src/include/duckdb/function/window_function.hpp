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
class BoundWindowFunction;

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

	BindWindowFunctionInput(ClientContext &context_p, BoundWindowFunction &bound_function_p,
	                        vector<unique_ptr<Expression>> &arguments_p, OptionalOrdering orders_p = nullptr,
	                        OptionalOrdering arg_orders_p = nullptr)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p), orders(orders_p),
	      arg_orders(arg_orders_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	BoundWindowFunction &GetBoundFunction() const {
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
	BoundWindowFunction &bound_function;
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
                                   const BoundWindowFunction &function);
typedef unique_ptr<FunctionData> (*window_deserialize_t)(Deserializer &deserializer, BoundWindowFunction &function);

class WindowFunctionCallbacks {
public:
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
};

class WindowFunctionProperties : public FunctionProperties {
public:
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
};

class WindowFunction : public SimpleFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr,
	               window_bounds_function_t bounds = nullptr, window_sharing_function_t sharing = nullptr,
	               window_global_function_t global = nullptr, window_local_function_t local = nullptr,
	               window_sink_function_t sink = nullptr, window_finalize_function_t finalize = nullptr,
	               window_evaluate_function_t evaluate = nullptr)
	    : SimpleFunction(name, arguments, return_type), window_enum(window_enum) {
		callbacks.bind = bind;
		callbacks.bounds = bounds;
		callbacks.sharing = sharing;
		callbacks.global = global;
		callbacks.local = local;
		callbacks.sink = sink;
		callbacks.finalize = finalize;
		callbacks.evaluate = evaluate;
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
	bool HasBindCallback() const { return callbacks.bind != nullptr; }
	window_bind_function_t GetBindCallback() const { return callbacks.bind; }
	void SetBindCallback(window_bind_function_t callback) { callbacks.bind = callback; }

	unique_ptr<BoundWindowExpression> Bind(ClientContext &context) const;
	unique_ptr<BoundWindowExpression> Bind(ClientContext &context, vector<unique_ptr<Expression>> arguments) const;

	bool HasBoundsCallback() const { return callbacks.bounds != nullptr; }
	window_bounds_function_t GetBoundsCallback() const { return callbacks.bounds; }
	void SetBoundsCallback(window_bounds_function_t callback) { callbacks.bounds = callback; }
	void GetBounds(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr) const {
		D_ASSERT(HasBoundsCallback());
		GetBoundsCallback()(bounds, wexpr);
	}

	bool HasSharingCallback() const { return callbacks.sharing != nullptr; }
	window_sharing_function_t GetSharingCallback() const { return callbacks.sharing; }
	void SetSharingCallback(window_sharing_function_t callback) { callbacks.sharing = callback; }
	void GetSharing(WindowExecutor &executor, WindowSharedExpressions &sharing) const {
		D_ASSERT(HasSharingCallback());
		GetSharingCallback()(executor, sharing);
	}

	bool HasGlobalCallback() const { return callbacks.global != nullptr; }
	window_global_function_t GetGlobalCallback() const { return callbacks.global; }
	void SetGlobalCallback(window_global_function_t callback) { callbacks.global = callback; }
	unique_ptr<GlobalSinkState> GetGlobalState(ClientContext &client, const WindowExecutor &executor,
								const idx_t payload_count,
								const ValidityMask &partition_mask,
								const ValidityMask &order_mask) const {
		D_ASSERT(HasGlobalCallback());
		return GetGlobalCallback()(client, executor, payload_count, partition_mask, order_mask);
	}

	bool HasLocalCallback() const { return callbacks.local != nullptr; }
	window_local_function_t GetLocalCallback() const { return callbacks.local; }
	void SetLocalCallback(window_local_function_t callback) { callbacks.local = callback; }
	unique_ptr<LocalSinkState> GetLocalState(ExecutionContext &context, const GlobalSinkState &gstate) const {
		D_ASSERT(HasLocalCallback());
		return GetLocalCallback()(context, gstate);
	}

	bool HasSinkCallback() const { return callbacks.sink != nullptr; }
	window_sink_function_t GetSinkCallback() const { return callbacks.sink; }
	void SetSinkCallback(window_sink_function_t callback) { callbacks.sink = callback; }
	void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
				    idx_t input_idx, OperatorSinkInput &sink) const {
		D_ASSERT(HasSinkCallback());
		return GetSinkCallback()(context, sink_chunk, coll_chunk, input_idx, sink);
	}

	bool HasFinalizeCallback() const { return callbacks.finalize != nullptr; }
	window_finalize_function_t GetFinalizeCallback() const { return callbacks.finalize; }
	void SetFinalizeCallback(window_finalize_function_t callback) { callbacks.finalize = callback; }
	void Finalize(ExecutionContext &context, optional_ptr<WindowCollection> collection, OperatorSinkInput &sink) const {
		D_ASSERT(HasFinalizeCallback());
		return GetFinalizeCallback()(context, collection, sink);
	}

	bool HasEvaluateCallback() const { return callbacks.evaluate != nullptr; }
	window_evaluate_function_t GetEvaluateCallback() const { return callbacks.evaluate; }
	void SetEvaluateCallback(window_evaluate_function_t callback) { callbacks.evaluate = callback; }
	void Evaluate(ExecutionContext &context, DataChunk &eval_chunk, DataChunk &bounds, Vector &result, idx_t row_idx,
				  OperatorSinkInput &sink) const {
		D_ASSERT(HasEvaluateCallback());
		return GetEvaluateCallback()(context, eval_chunk, bounds, result, row_idx, sink);
	}

	bool HasCanStreamCallback() const { return callbacks.can_stream != nullptr; }
	window_canstream_function_t GetCanStreamCallback() const { return callbacks.can_stream; }
	void SetCanStreamCallback(window_canstream_function_t callback) { callbacks.can_stream = callback; }
	bool CanStream(ClientContext &client, const BoundWindowExpression &wexpr, idx_t max_delta) const {
		D_ASSERT(HasCanStreamCallback());
		return GetCanStreamCallback()(client, wexpr, max_delta);
	}

	bool HasStreamingStateCallback() const { return callbacks.streaming_state != nullptr; }
	window_streaming_state_function_t GetStreamingStateCallback() const { return callbacks.streaming_state; }
	void SetStreamingStateCallback(window_streaming_state_function_t callback) { callbacks.streaming_state = callback; }
	unique_ptr<LocalSourceState> GetStreamingState(ClientContext &client, DataChunk &input,
												   const BoundWindowExpression &wexpr) const {
		D_ASSERT(HasStreamingStateCallback());
		return GetStreamingStateCallback()(client, input, wexpr);
	}

	bool HasStreamingDataCallback() const { return callbacks.stream != nullptr; }
	window_stream_function_t GetStreamingDataCallback() const { return callbacks.stream; }
	void SetStreamingDataCallback(window_stream_function_t callback) { callbacks.stream = callback; }
	void GetStreamingData(ExecutionContext &context, DataChunk &input, DataChunk &delayed, idx_t delayed_capacity,
						   Vector &result, LocalSourceState &lstate) const {
		D_ASSERT(HasStreamingDataCallback());
		GetStreamingDataCallback()(context, input, delayed, delayed_capacity, result, lstate);
	}

	bool HasSerializationCallbacks() const { return false; }
	void SetSerializeCallback(window_serialize_t callback) { callbacks.serialize = callback; }
	void SetDeserializeCallback(window_deserialize_t callback) { callbacks.deserialize = callback; }
	window_serialize_t GetSerializeCallback() const { return callbacks.serialize; }
	window_deserialize_t GetDeserializeCallback() const { return callbacks.deserialize; }
	// clang-format on

	//! The expression enum for the window function
	const ExpressionType window_enum;

public:
	bool CanDistinct() const {
		return properties.can_distinct;
	}
	bool CanFilter() const {
		return properties.can_filter;
	}
	bool CanOrderBy() const {
		return properties.can_order_by;
	}
	bool CanExclude() const {
		return properties.can_exclude;
	}
	bool CanIgnoreNulls() const {
		return properties.can_ignore_nulls;
	}
	void SetCanDistinct(bool value) {
		properties.can_distinct = value;
	}
	void SetCanFilter(bool value) {
		properties.can_filter = value;
	}
	void SetCanOrderBy(bool value) {
		properties.can_order_by = value;
	}
	void SetCanExclude(bool value) {
		properties.can_exclude = value;
	}
	void SetCanIgnoreNulls(bool value) {
		properties.can_ignore_nulls = value;
	}

protected:
	WindowFunctionProperties properties;
	WindowFunctionCallbacks callbacks;

	//! Additional function info, passed to the bind
	shared_ptr<WindowFunctionInfo> function_info;

public:
	// clang-format off
	FunctionStability GetStability() const { return properties.stability; }
	void SetStability(FunctionStability stability_p) { properties.stability = stability_p; }
	FunctionNullHandling GetNullHandling() const { return properties.null_handling; }
	void SetNullHandling(FunctionNullHandling null_handling_p) { properties.null_handling = null_handling_p; }
	FunctionErrors GetErrorMode() const { return properties.errors; }
	void SetErrorMode(FunctionErrors errors_p) { properties.errors = errors_p; }
	FunctionCollationHandling GetCollationHandling() const { return properties.collation_handling; }
	void SetCollationHandling(FunctionCollationHandling collation_handling_p) { properties.collation_handling = collation_handling_p; }

	//! Set this functions error-mode as fallible (can throw runtime errors)
	void SetFallible() { properties.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR; }
	//! Set this functions stability as volatile (can not be cached per row)
	void SetVolatile() { properties.stability = FunctionStability::VOLATILE; }
	// clang-format on
public:
	bool HasExtraFunctionInfo() const {
		return function_info != nullptr;
	}
	WindowFunctionInfo &GetExtraFunctionInfo() const {
		D_ASSERT(function_info.get());
		return *function_info;
	}
	void SetExtraFunctionInfo(shared_ptr<WindowFunctionInfo> info) {
		function_info = std::move(info);
	}
	template <class T, class... ARGS>
	void SetExtraFunctionInfo(ARGS &&... args) {
		function_info = make_shared_ptr<T>(std::forward<ARGS>(args)...);
	}

public:
	bool operator==(const WindowFunction &rhs) const {
		return name == rhs.name;
	}
	bool operator!=(const WindowFunction &rhs) const {
		return !(*this == rhs);
	}
};

class BoundWindowFunction : public WindowFunction {
public:
	BoundWindowFunction(const WindowFunction &base) : WindowFunction(base) { // NOLINT: allow implicit conversion
	}
};

} // namespace duckdb
