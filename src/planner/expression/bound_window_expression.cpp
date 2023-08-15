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

bool BoundWindowExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundWindowExpression>();

	if (ignore_nulls != other.ignore_nulls) {
		return false;
	}
	if (start != other.start || end != other.end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	// check if the filter expressions are equivalent
	if (!Expression::Equals(filter_expr, other.filter_expr)) {
		return false;
	}

	// check if the framing expressions are equivalent
	if (!Expression::Equals(start_expr, other.start_expr) || !Expression::Equals(end_expr, other.end_expr) ||
	    !Expression::Equals(offset_expr, other.offset_expr) || !Expression::Equals(default_expr, other.default_expr)) {
		return false;
	}

	return KeysAreCompatible(other);
}

bool BoundWindowExpression::KeysAreCompatible(const BoundWindowExpression &other) const {
	// check if the partitions are equivalent
	if (!Expression::ListEquals(partitions, other.partitions)) {
		return false;
	}
	// check if the orderings are equivalent
	if (orders.size() != other.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < orders.size(); i++) {
		if (!orders[i].Equals(other.orders[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundWindowExpression::Copy() {
	auto new_window = make_uniq<BoundWindowExpression>(type, return_type, nullptr, nullptr);
	new_window->CopyProperties(*this);

	if (aggregate) {
		new_window->aggregate = make_uniq<AggregateFunction>(*aggregate);
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
			new_window->partitions_stats.push_back(ps->ToUnique());
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
		aggregate = make_uniq<AggregateFunction>(std::move(aggr_function));
		return_type = aggregate->return_type;
	} else {
		children = reader.ReadRequiredSerializableList<Expression>(state.gstate);
		return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	}
	auto result = make_uniq<BoundWindowExpression>(state.type, return_type, std::move(aggregate), std::move(bind_info));

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

void BoundWindowExpression::FormatSerialize(FormatSerializer &serializer) const {
	Expression::FormatSerialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	if (type == ExpressionType::WINDOW_AGGREGATE) {
		D_ASSERT(aggregate);
		FunctionSerializer::FormatSerialize(serializer, *aggregate, bind_info.get());
	}
	serializer.WriteProperty(202, "partitions", partitions);
	serializer.WriteProperty(203, "orders", orders);
	serializer.WriteOptionalProperty(204, "filters", filter_expr);
	serializer.WriteProperty(205, "ignore_nulls", ignore_nulls);
	serializer.WriteProperty(206, "start", start);
	serializer.WriteProperty(207, "end", end);
	serializer.WriteOptionalProperty(208, "start_expr", start_expr);
	serializer.WriteOptionalProperty(209, "end_expr", end_expr);
	serializer.WriteOptionalProperty(210, "offset_expr", offset_expr);
	serializer.WriteOptionalProperty(211, "default_expr", default_expr);
}

unique_ptr<Expression> BoundWindowExpression::FormatDeserialize(FormatDeserializer &deserializer) {
	auto expression_type = deserializer.Get<ExpressionType>();
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (expression_type == ExpressionType::WINDOW_AGGREGATE) {
		auto entry = FunctionSerializer::FormatDeserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
		    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children);
		aggregate = make_uniq<AggregateFunction>(std::move(entry.first));
		bind_info = std::move(entry.second);
	}
	auto result =
	    make_uniq<BoundWindowExpression>(expression_type, return_type, std::move(aggregate), std::move(bind_info));
	deserializer.ReadProperty(202, "partitions", result->partitions);
	deserializer.ReadProperty(203, "orders", result->orders);
	deserializer.ReadOptionalProperty(204, "filters", result->filter_expr);
	deserializer.ReadProperty(205, "ignore_nulls", result->ignore_nulls);
	deserializer.ReadProperty(206, "start", result->start);
	deserializer.ReadProperty(207, "end", result->end);
	deserializer.ReadOptionalProperty(208, "start_expr", result->start_expr);
	deserializer.ReadOptionalProperty(209, "end_expr", result->end_expr);
	deserializer.ReadOptionalProperty(210, "offset_expr", result->offset_expr);
	deserializer.ReadOptionalProperty(211, "default_expr", result->default_expr);
	return std::move(result);
}

} // namespace duckdb
