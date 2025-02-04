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
      bind_info(std::move(bind_info)), ignore_nulls(false), distinct(false) {
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
	if (distinct != other.distinct) {
		return false;
	}
	if (start != other.start || end != other.end) {
		return false;
	}
	if (exclude_clause != other.exclude_clause) {
		return false;
	}

	// If there are aggregates, check they are equal
	if (aggregate.get() != other.aggregate.get()) {
		if (!aggregate || !other.aggregate || *aggregate != *other.aggregate) {
			return false;
		}
	}
	// If there's function data, check if they are equal
	if (bind_info.get() != other.bind_info.get()) {
		if (!bind_info || !other.bind_info || !bind_info->Equals(*other.bind_info)) {
			return false;
		}
	}
	// check if the child expressions are equivalent
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	if (!Expression::ListEquals(partitions, other.partitions)) {
		return false;
	}
	// check if the filter expressions are equivalent
	if (!Expression::Equals(filter_expr, other.filter_expr)) {
		return false;
	}

	// check if the argument orderings are equivalent
	if (arg_orders.size() != other.arg_orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < arg_orders.size(); i++) {
		if (!arg_orders[i].Equals(other.arg_orders[i])) {
			return false;
		}
	}

	// check if the framing expressions are equivalent
	if (!Expression::Equals(start_expr, other.start_expr) || !Expression::Equals(end_expr, other.end_expr) ||
	    !Expression::Equals(offset_expr, other.offset_expr) || !Expression::Equals(default_expr, other.default_expr)) {
		return false;
	}

	return KeysAreCompatible(other);
}

bool BoundWindowExpression::PartitionsAreEquivalent(const BoundWindowExpression &other) const {
	// Partitions are not order sensitive.
	if (partitions.size() != other.partitions.size()) {
		return false;
	}
	// TODO: Should partitions be an expression_set_t?
	expression_set_t others;
	for (const auto &partition : other.partitions) {
		others.insert(*partition);
	}
	for (const auto &partition : partitions) {
		if (!others.count(*partition)) {
			return false;
		}
	}
	return true;
}

idx_t BoundWindowExpression::GetSharedOrders(const vector<BoundOrderByNode> &lhs, const vector<BoundOrderByNode> &rhs) {
	const auto overlap = MinValue<idx_t>(lhs.size(), rhs.size());

	idx_t result = 0;
	for (; result < overlap; ++result) {
		if (!lhs.at(result).Equals(rhs.at(result))) {
			return 0;
		}
	}

	return result;
}

idx_t BoundWindowExpression::GetSharedOrders(const BoundWindowExpression &other) const {
	return GetSharedOrders(orders, other.orders);
}

bool BoundWindowExpression::KeysAreCompatible(const BoundWindowExpression &other) const {
	if (!PartitionsAreEquivalent(other)) {
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

unique_ptr<Expression> BoundWindowExpression::Copy() const {
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

	for (auto &o : arg_orders) {
		new_window->arg_orders.emplace_back(o.type, o.null_order, o.expression->Copy());
	}

	new_window->filter_expr = filter_expr ? filter_expr->Copy() : nullptr;

	new_window->start = start;
	new_window->end = end;
	new_window->exclude_clause = exclude_clause;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;
	new_window->ignore_nulls = ignore_nulls;
	new_window->distinct = distinct;

	for (auto &es : expr_stats) {
		if (es) {
			new_window->expr_stats.push_back(es->ToUnique());
		} else {
			new_window->expr_stats.push_back(nullptr);
		}
	}
	return std::move(new_window);
}

void BoundWindowExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	if (type == ExpressionType::WINDOW_AGGREGATE) {
		D_ASSERT(aggregate);
		FunctionSerializer::Serialize(serializer, *aggregate, bind_info.get());
	}
	serializer.WriteProperty(202, "partitions", partitions);
	serializer.WriteProperty(203, "orders", orders);
	serializer.WritePropertyWithDefault(204, "filters", filter_expr, unique_ptr<Expression>());
	serializer.WriteProperty(205, "ignore_nulls", ignore_nulls);
	serializer.WriteProperty(206, "start", start);
	serializer.WriteProperty(207, "end", end);
	serializer.WritePropertyWithDefault(208, "start_expr", start_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(209, "end_expr", end_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(210, "offset_expr", offset_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(211, "default_expr", default_expr, unique_ptr<Expression>());
	serializer.WriteProperty(212, "exclude_clause", exclude_clause);
	serializer.WriteProperty(213, "distinct", distinct);
	serializer.WriteProperty(214, "arg_orders", arg_orders);
}

unique_ptr<Expression> BoundWindowExpression::Deserialize(Deserializer &deserializer) {
	auto expression_type = deserializer.Get<ExpressionType>();
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (expression_type == ExpressionType::WINDOW_AGGREGATE) {
		auto entry = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
		    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, return_type);
		aggregate = make_uniq<AggregateFunction>(std::move(entry.first));
		bind_info = std::move(entry.second);
	}
	auto result =
	    make_uniq<BoundWindowExpression>(expression_type, return_type, std::move(aggregate), std::move(bind_info));
	result->children = std::move(children);
	deserializer.ReadProperty(202, "partitions", result->partitions);
	deserializer.ReadProperty(203, "orders", result->orders);
	deserializer.ReadPropertyWithExplicitDefault(204, "filters", result->filter_expr, unique_ptr<Expression>());
	deserializer.ReadProperty(205, "ignore_nulls", result->ignore_nulls);
	deserializer.ReadProperty(206, "start", result->start);
	deserializer.ReadProperty(207, "end", result->end);
	deserializer.ReadPropertyWithExplicitDefault(208, "start_expr", result->start_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithExplicitDefault(209, "end_expr", result->end_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithExplicitDefault(210, "offset_expr", result->offset_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithExplicitDefault(211, "default_expr", result->default_expr, unique_ptr<Expression>());
	deserializer.ReadProperty(212, "exclude_clause", result->exclude_clause);
	deserializer.ReadProperty(213, "distinct", result->distinct);
	deserializer.ReadPropertyWithExplicitDefault(214, "arg_orders", result->arg_orders, vector<BoundOrderByNode>());
	return std::move(result);
}

} // namespace duckdb
