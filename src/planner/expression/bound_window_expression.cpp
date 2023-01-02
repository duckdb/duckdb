#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_serialization.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

namespace duckdb {

BoundWindowExpression::BoundWindowExpression(ExpressionType type, LogicalType return_type,
                                             unique_ptr<AggregateFunction> aggregate,
                                             unique_ptr<FunctionData> bind_info)
    : Expression(type, ExpressionClass::BOUND_WINDOW, std::move(return_type)), aggregate(std::move(aggregate)),
      bind_info(std::move(bind_info)), ignore_nulls(false) {
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

	return std::move(new_window);
}

void BoundWindowExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField<bool>(aggregate.get());
	if (aggregate) {
		D_ASSERT(return_type == aggregate->return_type);
		FunctionSerializer::Serialize<AggregateFunction>(writer, *aggregate, return_type, children, bind_info.get());
	} else {
		// children and return_type are written as part of the aggregate function otherwise
		writer.WriteSerializableList(children);
		writer.WriteSerializable(return_type);
	}
	writer.WriteSerializableList(partitions);
	writer.WriteRegularSerializableList(orders);
	// FIXME: partitions_stats
	writer.WriteOptional(filter_expr);
	writer.WriteField<bool>(ignore_nulls);
	writer.WriteField<WindowBoundary>(start);
	writer.WriteField<WindowBoundary>(end);
	writer.WriteOptional(start_expr);
	writer.WriteOptional(end_expr);
	writer.WriteOptional(offset_expr);
	writer.WriteOptional(default_expr);
}

unique_ptr<Expression> BoundWindowExpression::Deserialize(ExpressionDeserializationState &state, FieldReader &reader) {
	auto has_aggregate = reader.ReadRequired<bool>();
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	vector<unique_ptr<Expression>> children;
	LogicalType return_type;
	if (has_aggregate) {
		auto aggr_function = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
		    reader, state, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, bind_info);
		aggregate = make_unique<AggregateFunction>(std::move(aggr_function));
		return_type = aggregate->return_type;
	} else {
		children = reader.ReadRequiredSerializableList<Expression>(state.gstate);
		return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	}
	auto result = make_unique<BoundWindowExpression>(state.type, return_type, std::move(aggregate), std::move(bind_info));

	result->partitions = reader.ReadRequiredSerializableList<Expression>(state.gstate);
	result->orders = reader.ReadRequiredSerializableList<BoundOrderByNode, BoundOrderByNode>(state.gstate);
	result->filter_expr = reader.ReadOptional<Expression>(nullptr, state.gstate);
	result->ignore_nulls = reader.ReadRequired<bool>();
	result->start = reader.ReadRequired<WindowBoundary>();
	result->end = reader.ReadRequired<WindowBoundary>();
	result->start_expr = reader.ReadOptional<Expression>(nullptr, state.gstate);
	result->end_expr = reader.ReadOptional<Expression>(nullptr, state.gstate);
	result->offset_expr = reader.ReadOptional<Expression>(nullptr, state.gstate);
	result->default_expr = reader.ReadOptional<Expression>(nullptr, state.gstate);
	result->children = std::move(children);
	return std::move(result);
}

} // namespace duckdb
