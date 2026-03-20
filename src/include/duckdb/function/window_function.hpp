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
                                                           vector<unique_ptr<Expression>> &arguments,
                                                           vector<OrderByNode> &orders,
                                                           vector<OrderByNode> &arg_orders);

class WindowFunction : public BaseScalarFunction { // NOLINT: work-around bug in clang-tidy
public:
	WindowFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	               ExpressionType window_enum, window_bind_function_t bind = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind) {
	}

	WindowFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, ExpressionType window_enum,
	               window_bind_function_t bind = nullptr)
	    : BaseScalarFunction(string(), arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), FunctionNullHandling::DEFAULT_NULL_HANDLING),
	      window_enum(window_enum), bind(bind) {
	}

	// clang-format off
	bool HasBindCallback() const { return bind != nullptr; }
	window_bind_function_t GetBindCallback() const { return bind; }
	void SetBindCallback(window_bind_function_t callback) { bind = callback; }
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
