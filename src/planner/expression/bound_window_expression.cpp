#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/parser/expression/window_expression.hpp"

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_serialization.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundWindowExpression::BoundWindowExpression(LogicalType return_type, unique_ptr<BoundAggregateFunction> aggregate,
                                             unique_ptr<BoundWindowFunction> window, unique_ptr<FunctionData> bind_info)
    : Expression(window.get() ? window->window_enum : ExpressionType::WINDOW_AGGREGATE, ExpressionClass::BOUND_WINDOW,
                 std::move(return_type)),
      aggregate(std::move(aggregate)), window(std::move(window)), bind_info(std::move(bind_info)), ignore_nulls(false),
      distinct(false) {
}

string BoundWindowExpression::ToString() const {
	string function_name = aggregate.get() ? aggregate->GetName() : window->GetName();
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
	// If there are windows, check they are equal
	if (window.get() != other.window.get()) {
		if (!window || !other.window || *window != *other.window) {
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
	if (!Expression::Equals(start_expr, other.start_expr) || !Expression::Equals(end_expr, other.end_expr)) {
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
	unique_ptr<BoundAggregateFunction> agg_copy;
	if (aggregate) {
		agg_copy = make_uniq<BoundAggregateFunction>(*aggregate);
	}
	unique_ptr<BoundWindowFunction> win_copy;
	if (window) {
		win_copy = make_uniq<BoundWindowFunction>(*window);
	}
	unique_ptr<FunctionData> bind_copy;
	if (bind_info) {
		bind_copy = bind_info->Copy();
	}
	auto new_window =
	    make_uniq<BoundWindowExpression>(return_type, std::move(agg_copy), std::move(win_copy), std::move(bind_copy));
	new_window->CopyProperties(*this);
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

vector<unique_ptr<Expression>> BoundWindowExpression::SerializedChildren(Serializer &serializer) const {
	vector<unique_ptr<Expression>> result;
	idx_t nargs = children.size();
	if (!serializer.ShouldSerialize(8) && window) {
		const auto &function_name = window->GetName();
		if (function_name == "lead" || function_name == "lag") {
			nargs = 1;
		}
	}

	for (idx_t i = 0; i < nargs; ++i) {
		result.emplace_back(children[i]->Copy());
	}

	return result;
}

unique_ptr<Expression> BoundWindowExpression::SerializedOffset(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(8) && children.size() > 1 && window) {
		const auto &function_name = window->GetName();
		if (function_name == "lead" || function_name == "lag") {
			return children[1]->Copy();
		}
	}

	return nullptr;
}

unique_ptr<Expression> BoundWindowExpression::SerializedDefault(Serializer &serializer) const {
	if (!serializer.ShouldSerialize(8) && children.size() > 2 && window) {
		const auto &function_name = window->GetName();
		if (function_name == "lead" || function_name == "lag") {
			return children[2]->Copy();
		}
	}

	return nullptr;
}

void BoundWindowExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", SerializedChildren(serializer));
	if (type == ExpressionType::WINDOW_AGGREGATE) {
		D_ASSERT(aggregate);
		FunctionSerializer::Serialize(serializer, *aggregate, bind_info.get());
	} else if (type == ExpressionType::WINDOW_FUNCTION) {
		//	New window function. Older versions will treat it as an unknown aggregate.
		D_ASSERT(window);
		FunctionSerializer::Serialize(serializer, *window, bind_info.get());
	} // else the expression type is all we need as there is no binding info.
	auto null_expr = unique_ptr<Expression>();
	serializer.WriteProperty(202, "partitions", partitions);
	serializer.WriteProperty(203, "orders", orders);
	serializer.WritePropertyWithDefault(204, "filters", filter_expr, unique_ptr<Expression>());
	serializer.WriteProperty(205, "ignore_nulls", ignore_nulls);
	serializer.WriteProperty(206, "start", start);
	serializer.WriteProperty(207, "end", end);
	serializer.WritePropertyWithDefault(208, "start_expr", start_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(209, "end_expr", end_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(210, "offset_expr", SerializedOffset(serializer), unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(211, "default_expr", SerializedDefault(serializer), unique_ptr<Expression>());
	serializer.WriteProperty(212, "exclude_clause", exclude_clause);
	serializer.WriteProperty(213, "distinct", distinct);
	serializer.WriteProperty(214, "arg_orders", arg_orders);
}

unique_ptr<Expression> BoundWindowExpression::Deserialize(Deserializer &deserializer) {
	auto expression_type = deserializer.Get<ExpressionType>();
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	unique_ptr<BoundAggregateFunction> aggregate;
	unique_ptr<BoundWindowFunction> window;
	unique_ptr<FunctionData> bind_info;
	if (expression_type == ExpressionType::WINDOW_AGGREGATE) {
		auto entry = FunctionSerializer::Deserialize<BoundAggregateFunction, AggregateFunctionCatalogEntry>(
		    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, return_type);
		aggregate = make_uniq<BoundAggregateFunction>(std::move(entry.first));
		bind_info = std::move(entry.second);
	} else if (expression_type == ExpressionType::WINDOW_FUNCTION) {
		//	New window function
		auto entry = FunctionSerializer::Deserialize<BoundWindowFunction, WindowFunctionCatalogEntry>(
		    deserializer, CatalogType::WINDOW_FUNCTION_ENTRY, children, return_type);
		window = make_uniq<BoundWindowFunction>(std::move(entry.first));
		bind_info = std::move(entry.second);
	}
	auto result =
	    make_uniq<BoundWindowExpression>(return_type, std::move(aggregate), std::move(window), std::move(bind_info));
	unique_ptr<Expression> expr;
	result->children = std::move(children);
	deserializer.ReadProperty(202, "partitions", result->partitions);
	deserializer.ReadProperty(203, "orders", result->orders);
	deserializer.ReadPropertyWithExplicitDefault(204, "filters", result->filter_expr, unique_ptr<Expression>());
	deserializer.ReadProperty(205, "ignore_nulls", result->ignore_nulls);
	deserializer.ReadProperty(206, "start", result->start);
	deserializer.ReadProperty(207, "end", result->end);
	deserializer.ReadPropertyWithExplicitDefault(208, "start_expr", result->start_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithExplicitDefault(209, "end_expr", result->end_expr, unique_ptr<Expression>());
	// offset and default are now just children
	deserializer.ReadPropertyWithExplicitDefault(210, "offset_expr", expr, unique_ptr<Expression>());
	if (expr) {
		result->children.emplace_back(std::move(expr));
	}
	deserializer.ReadPropertyWithExplicitDefault(211, "default_expr", expr, unique_ptr<Expression>());
	if (expr) {
		result->children.emplace_back(std::move(expr));
	}
	deserializer.ReadProperty(212, "exclude_clause", result->exclude_clause);
	deserializer.ReadProperty(213, "distinct", result->distinct);
	deserializer.ReadPropertyWithExplicitDefault(214, "arg_orders", result->arg_orders, vector<BoundOrderByNode>());

	//	Builtin window functions didn't used to be serialized, so we need to look them up in the system catalog
	if (!result->aggregate && !result->window) {
		const auto name = WindowExpression::ExpressionTypeToWindow(expression_type);
		vector<LogicalType> arguments;
		arguments.reserve(result->children.size());
		for (auto &child : result->children) {
			arguments.push_back(child->GetReturnType());
		}

		auto &context = deserializer.Get<ClientContext &>();
		auto binder = Binder::CreateBinder(context);
		EntryLookupInfo lookup(CatalogType::SCALAR_FUNCTION_ENTRY, name);
		auto entry = binder->GetCatalogEntry(SYSTEM_CATALOG, DEFAULT_SCHEMA, lookup, OnEntryNotFound::THROW_EXCEPTION);
		auto &func = entry->Cast<WindowFunctionCatalogEntry>();

		FunctionBinder function_binder(*binder);
		ErrorData error_win;
		auto best = function_binder.BindFunction(func.name, func.functions, arguments, error_win);
		if (!best.IsValid()) {
			error_win.Throw();
		}

		const auto &win_func = func.functions.GetFunctionByOffset(best.GetIndex());

		auto [bound_func, bind_info] = function_binder.ResolveFunction(win_func, result->children);

		result->bind_info = std::move(bind_info);
		result->type = expression_type;
		result->window = make_uniq<BoundWindowFunction>(std::move(bound_func));
	}

	return std::move(result);
}

} // namespace duckdb
