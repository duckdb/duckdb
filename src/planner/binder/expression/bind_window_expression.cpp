#include "parser/expression/window_expression.hpp"
#include "planner/expression/bound_window_expression.hpp"
#include "planner/expression_binder.hpp"

using namespace duckdb;
using namespace std;

BindResult ExpressionBinder::Bind(WindowExpression &window, uint32_t depth) {
	if (inside_window) {
		return BindResult(move(expr), "window function calls cannot be nested");
	}
	if (depth > 0) {
		return BindResult(move(expr), "correlated columns in window functions not supported");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent nested window functions
	this->inside_window = true;
	auto bind_result = BindChildren(move(expr), depth);
	this->inside_window = false;
	if (bind_result.HasError()) {
		// failed to bind children of window function
		return bind_result;
	}
	bind_result.expression->ResolveType();
	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(
	    *bind_result.expression, bind_result.expression->return_type,
	    ColumnBinding(node.binding.window_index, node.binding.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.binding.windows.push_back(move(bind_result.expression));
	return BindResult(move(colref));
}

// BindResult SelectBinder::BindWindow(unique_ptr<Expression> expr, uint32_t depth) {
// 	assert(expr && expr->GetExpressionClass() == ExpressionClass::WINDOW);
// 	if (inside_window) {
// 		return BindResult(move(expr), "window function calls cannot be nested");
// 	}
// 	if (depth > 0) {
// 		return BindResult(move(expr), "correlated columns in window functions not supported");
// 	}
// 	// bind inside the children of the window function
// 	// we set the inside_window flag to true to prevent nested window functions
// 	this->inside_window = true;
// 	auto bind_result = BindChildren(move(expr), depth);
// 	this->inside_window = false;
// 	if (bind_result.HasError()) {
// 		// failed to bind children of window function
// 		return bind_result;
// 	}
// 	bind_result.expression->ResolveType();
// 	// create a BoundColumnRef that references this entry
// 	auto colref = make_unique<BoundColumnRefExpression>(
// 	    *bind_result.expression, bind_result.expression->return_type,
// 	    ColumnBinding(node.binding.window_index, node.binding.windows.size()), depth);
// 	// move the WINDOW expression into the set of bound windows
// 	node.binding.windows.push_back(move(bind_result.expression));
// 	return BindResult(move(colref));
// }
