//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

class BoundWindowExpression;
struct WindowSharedExpressions;
class WindowExecutor;
class GlobalSinkState;

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
	BindWindowFunctionInput(ClientContext &context_p, WindowFunction &bound_function_p,
	                        vector<unique_ptr<Expression>> &arguments_p)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p) {
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

private:
	ClientContext &context;
	WindowFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
};

//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*window_bind_function_t)(BindWindowFunctionInput &input);

//! Validates the additional ordering usage.
typedef void (*window_validate_function_t)(ClientContext &context, WindowFunction &function,
                                           vector<unique_ptr<Expression>> &arguments, vector<OrderByNode> &orders,
                                           vector<OrderByNode> &arg_orders);

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

//! Serialization of the binding data (if any)
typedef void (*window_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                   const WindowFunction &function);
typedef unique_ptr<FunctionData> (*window_deserialize_t)(Deserializer &deserializer, WindowFunction &function);

class WindowFunction : public BaseScalarFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr,
	               window_bounds_function_t bounds = nullptr, window_sharing_function_t sharing = nullptr,
	               window_global_function_t global = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind), bounds(bounds), sharing(sharing), global(global) {
	}

	WindowFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, ExpressionType window_enum,
	               window_bind_function_t bind = nullptr, window_bounds_function_t bounds = nullptr,
	               window_sharing_function_t sharing = nullptr, window_global_function_t global = nullptr)
	    : WindowFunction(string(), arguments, return_type, window_enum, bind, bounds, sharing, global) {
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


	bool HasValidateCallback() const { return validate != nullptr; }
	window_validate_function_t GetValidateCallback() const { return validate; }
	void SetValidateCallback(window_validate_function_t callback) { validate = callback; }

	bool HasBoundsCallback() const { return bounds != nullptr; }
	window_bounds_function_t GetBoundsCallback() const { return bounds; }
	void SetBoundsCallback(window_bounds_function_t callback) { bounds = callback; }

	bool HasSharingCallback() const { return sharing != nullptr; }
	window_sharing_function_t GetSharingCallback() const { return sharing; }
	void SetSharingCallback(window_sharing_function_t callback) { sharing = callback; }

	bool HasGlobalCallback() const { return global != nullptr; }
	window_global_function_t GetGlobalCallback() const { return global; }
	void SetGlobalCallback(window_global_function_t callback) { global = callback; }

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
	//! The sort validation function
	window_validate_function_t validate = nullptr;
	//! The framing bounds lists
	window_bounds_function_t bounds = nullptr;
	//! The children sharing requirements
	window_sharing_function_t sharing = nullptr;
	//! The global state constructor
	window_global_function_t global = nullptr;

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
