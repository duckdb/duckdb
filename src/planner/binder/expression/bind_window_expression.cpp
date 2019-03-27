#include "parser/expression/window_expression.hpp"
#include "planner/expression/bound_window_expression.hpp"
#include "planner/select_binder.hpp"

using namespace duckdb;
using namespace std;

static SQLType ResolveWindowExpressionType(ExpressionType window_type, SQLType child_type) {
	switch (window_type) {
	case ExpressionType::WINDOW_SUM:
		switch (child_type) {
		case TypeId::BOOLEAN:
		case TypeId::TINYINT:
		case TypeId::SMALLINT:
		case TypeId::INTEGER:
		case TypeId::BIGINT:
			return SQLType(SQLTypeId::BIGINT);
		default:
			return child_type;
		}
		break;
	case ExpressionType::WINDOW_AVG:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_CUME_DIST:
		return SQLType(SQLTypeId::DECIMAL);
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_COUNT_STAR:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_NTILE:
			return SQLType(SQLTypeId::BIGINT);
	case ExpressionType::WINDOW_MIN:
	case ExpressionType::WINDOW_MAX:
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
		assert(child_type.id != SQLTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	case ExpressionType::WINDOW_LEAD:
	default:
		assert(type == ExpressionType::WINDOW_LAG);
		assert(child_type.id != SQLTypeId::INVALID); // "Window function needs an expression"
		return child_type;
	}
}

BindResult SelectBinder::BindWindow(WindowExpression &window, uint32_t depth) {
	if (inside_window) {
		return BindResult("window function calls cannot be nested");
	}
	if (depth > 0) {
		return BindResult("correlated columns in window functions not supported");
	}
	// bind inside the children of the window function
	// we set the inside_window flag to true to prevent binding nested window functions
	this->inside_window = true;
	string error;
	BindChild(window.child, depth, error);
	for(auto &child : window.partitions) {
		BindChild(child, depth, error);
	}
	for(auto &order : window.orders) {
		BindChild(order.expression, depth, error);
	}
	BindChild(window.start_expr, depth, error);
	BindChild(window.end_expr, depth, error);
	BindChild(window.offset_expr, depth, error);
	BindChild(window.default_expr, depth, error);
	this->inside_window = false;
	if (!error.empty()) {
		// failed to bind children of window function
		return BindResult(error);
	}
	// fetch the children
	auto child = GetExpression(window.child);
	SQLType sql_type = ResolveWindowExpressionType(window.type, child ? child->sql_type : SQLType());
	auto result = make_unique<BoundWindowExpression>(window.type, GetInternalType(sql_type), sql_type);
	result->child = move(child);
	for(auto &child : window.partitions) {
		result->partitions.push_back(GetExpression(child));
	}
	for(auto &order : window.orders) {
		BoundOrderByNode bound_order;
		bound_order.expression = GetExpression(order.expression);
		bound_order.type = order.type;
		result->orders.push_back(move(bound_order));
	}
	result->start_expr = GetExpression(window.start_expr);
	result->end_expr = GetExpression(window.end_expr);
	result->offset_expr = GetExpression(window.offset_expr);
	result->default_expr = GetExpression(window.default_expr);
	// create a BoundColumnRef that references this entry
	auto colref = make_unique<BoundColumnRefExpression>(
	    *window.GetName(), result->return_type,
	    ColumnBinding(node.window_index, node.windows.size()), depth);
	// move the WINDOW expression into the set of bound windows
	node.windows.push_back(move(result));
	return BindResult(move(colref));
}