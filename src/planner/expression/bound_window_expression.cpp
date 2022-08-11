#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/aggregate_function.hpp"

namespace duckdb {

BoundWindowExpression::BoundWindowExpression(ExpressionType type, LogicalType return_type,
                                             unique_ptr<AggregateFunction> aggregate,
                                             unique_ptr<FunctionData> bind_info)
    : Expression(type, ExpressionClass::BOUND_WINDOW, move(return_type)), aggregate(move(aggregate)),
      bind_info(move(bind_info)), ignore_nulls(false) {
}

string BoundWindowExpression::ToString() const {
	string function_name = aggregate.get() ? aggregate->name : ExpressionTypeToString(type);
	return WindowExpression::ToString<BoundWindowExpression, Expression, BoundOrderByNode>(*this, string(),
	                                                                                       function_name);
}

bool BoundWindowExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundWindowExpression *)other_p;

	if (ignore_nulls != other->ignore_nulls) {
		return false;
	}
	if (start != other->start || end != other->end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (other->children.size() != children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	// check if the filter expressions are equivalent
	if (!Expression::Equals(filter_expr.get(), other->filter_expr.get())) {
		return false;
	}

	// check if the framing expressions are equivalent
	if (!Expression::Equals(start_expr.get(), other->start_expr.get()) ||
	    !Expression::Equals(end_expr.get(), other->end_expr.get()) ||
	    !Expression::Equals(offset_expr.get(), other->offset_expr.get()) ||
	    !Expression::Equals(default_expr.get(), other->default_expr.get())) {
		return false;
	}

	return KeysAreCompatible(other);
}

bool BoundWindowExpression::KeysAreCompatible(const BoundWindowExpression *other) const {
	// check if the partitions are equivalent
	if (partitions.size() != other->partitions.size()) {
		return false;
	}
	for (idx_t i = 0; i < partitions.size(); i++) {
		if (!Expression::Equals(partitions[i].get(), other->partitions[i].get())) {
			return false;
		}
	}
	// check if the orderings are equivalent
	if (orders.size() != other->orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < orders.size(); i++) {
		if (orders[i].type != other->orders[i].type) {
			return false;
		}
		if (!BaseExpression::Equals((BaseExpression *)orders[i].expression.get(),
		                            (BaseExpression *)other->orders[i].expression.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundWindowExpression::Copy() {
	auto new_window = make_unique<BoundWindowExpression>(type, return_type, nullptr, nullptr);
	new_window->CopyProperties(*this);

	if (aggregate) {
		new_window->aggregate = make_unique<AggregateFunction>(*aggregate);
	}
	if (bind_info) {
		new_window->bind_info = bind_info->Copy();
	}
	for (auto &child : children) {
		new_window->children.push_back(child->Copy());
	}
	for (auto &e : partitions) {
		new_window->partitions.push_back(e->Copy());
	}
	for (auto &ps : partitions_stats) {
		if (ps) {
			new_window->partitions_stats.push_back(ps->Copy());
		} else {
			new_window->partitions_stats.push_back(nullptr);
		}
	}
	for (auto &o : orders) {
		new_window->orders.emplace_back(o.type, o.null_order, o.expression->Copy());
	}

	new_window->filter_expr = filter_expr ? filter_expr->Copy() : nullptr;

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;
	new_window->ignore_nulls = ignore_nulls;

	return move(new_window);
}

//	//! The bound aggregate function
//	unique_ptr<AggregateFunction> aggregate;
//	//! The bound function info
//	unique_ptr<FunctionData> bind_info;
//	//! The child expressions of the main window function
//	vector<unique_ptr<Expression>> children;
//	//! The set of expressions to partition by
//	vector<unique_ptr<Expression>> partitions;
//	//! Statistics belonging to the partitions expressions
//	vector<unique_ptr<BaseStatistics>> partitions_stats;
//	//! The set of ordering clauses
//	vector<BoundOrderByNode> orders;
//	//! Expression representing a filter, only used for aggregates
//	unique_ptr<Expression> filter_expr;
//	//! True to ignore NULL values
//	bool ignore_nulls;
//	//! The window boundaries
//	WindowBoundary start = WindowBoundary::INVALID;
//	WindowBoundary end = WindowBoundary::INVALID;
//
//	unique_ptr<Expression> start_expr;
//	unique_ptr<Expression> end_expr;
//	//! Offset and default expressions for WINDOW_LEAD and WINDOW_LAG functions
//	unique_ptr<Expression> offset_expr;
//	unique_ptr<Expression> default_expr;
//

void BoundWindowExpression::Serialize(FieldWriter &writer) const {
	if (aggregate) {
		throw NotImplementedException("FIXME: serialize aggregate function");
	}
	//	// FIXME: aggregate
	//	writer.WriteSerializableList(children);
	//	writer.WriteSerializableList(partitions);
	//	writer.WriteRegularSerializableList(orders);
	//	// FIXME: partitions_stats
	//	writer.WriteOptional(filter_expr);
	//	writer.WriteField<bool>(ignore_nulls);
	//	writer.WriteField<WindowBoundary>(start);
	//	writer.WriteField<WindowBoundary>(end);
	//	writer.WriteOptional(start_expr);
	//	writer.WriteOptional(end_expr);
	//	writer.WriteOptional(offset_expr);
	//	writer.WriteOptional(default_expr);
	throw NotImplementedException("FIXME: serialize window function");
}

unique_ptr<Expression> BoundWindowExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	throw NotImplementedException("FIXME: deserialize window function");
	//	auto expression_type = reader.ReadRequired<ExpressionType>();
	//	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	//	auto children = reader.ReadRequiredSerializableList<Expression>(context);
	//
	//	auto result = make_unique<BoundWindowExpression>(expression_type, return_type);
	//	result->children = move(children);
	//	return move(result);
}

} // namespace duckdb
