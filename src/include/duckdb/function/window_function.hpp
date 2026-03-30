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

//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*window_bind_function_t)(ClientContext &context, WindowFunction &function,
                                                           vector<unique_ptr<Expression>> &arguments);

//! Validates the additional ordering usage.
typedef void (*window_validate_function_t)(ClientContext &context, WindowFunction &function,
                                           vector<unique_ptr<Expression>> &arguments, vector<OrderByNode> &orders,
                                           vector<OrderByNode> &arg_orders);

//! Requests framing bounds that the function uses
typedef void (*window_bounds_function_t)(WindowBoundsSet &bounds, const BoundWindowExpression &wexpr);

//! Serialization of the binding data (if any)
typedef void (*window_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                   const WindowFunction &function);
typedef unique_ptr<FunctionData> (*window_deserialize_t)(Deserializer &deserializer, WindowFunction &function);

class WindowFunction : public BaseScalarFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr,
	               window_bounds_function_t bounds = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind), bounds(bounds) {
	}

	WindowFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, ExpressionType window_enum,
	               window_bind_function_t bind = nullptr, window_bounds_function_t bounds = nullptr)
	    : BaseScalarFunction(string(), arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind), bounds(bounds) {
	}

	// clang-format off
	bool HasBindCallback() const { return bind != nullptr; }
	window_bind_function_t GetBindCallback() const { return bind; }
	void SetBindCallback(window_bind_function_t callback) { bind = callback; }

	bool HasValidateCallback() const { return validate != nullptr; }
	window_validate_function_t GetValidateCallback() const { return validate; }
	void SetValidateCallback(window_validate_function_t callback) { validate = callback; }

	bool HasBoundsCallback() const { return bounds != nullptr; }
	window_bounds_function_t GetBoundsCallback() const { return bounds; }
	void SetBoundsCallback(window_bounds_function_t callback) { bounds = callback; }

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

	//! The bind function (may be null)
	window_bind_function_t bind = nullptr;
	//! The sort validation function
	window_validate_function_t validate = nullptr;
	//! The framing bounds lists
	window_bounds_function_t bounds = nullptr;

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
