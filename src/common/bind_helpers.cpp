#include "duckdb/common/bind_helpers.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/main/config.hpp"

#include <numeric>

namespace duckdb {

Value ConvertVectorToValue(vector<Value> set) {
	if (set.empty()) {
		return Value::LIST(LogicalType::BOOLEAN, std::move(set));
	}
	return Value::LIST(std::move(set));
}

vector<bool> ParseColumnList(const vector<Value> &set, vector<string> &names, const string &loption) {
	vector<bool> result;

	if (set.empty()) {
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	// list of options: parse the list
	case_insensitive_map_t<bool> option_map;
	for (idx_t i = 0; i < set.size(); i++) {
		option_map[set[i].ToString()] = false;
	}
	result.resize(names.size(), false);
	for (idx_t i = 0; i < names.size(); i++) {
		auto entry = option_map.find(names[i]);
		if (entry != option_map.end()) {
			result[i] = true;
			entry->second = true;
		}
	}
	for (auto &entry : option_map) {
		if (!entry.second) {
			throw BinderException("\"%s\" expected to find %s, but it was not found in the table", loption,
			                      entry.first.c_str());
		}
	}
	return result;
}

vector<bool> ParseColumnList(const Value &value, vector<string> &names, const string &loption) {
	vector<bool> result;

	// Only accept a list of arguments
	if (value.type().id() != LogicalTypeId::LIST) {
		// Support a single argument if it's '*'
		if (value.type().id() == LogicalTypeId::VARCHAR && value.GetValue<string>() == "*") {
			result.resize(names.size(), true);
			return result;
		}
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	if (value.IsNull()) {
		throw BinderException("\"%s\" expects a column list or * as parameter, it can't be a NULL value", loption);
	}
	auto &children = ListValue::GetChildren(value);
	// accept '*' as single argument
	if (children.size() == 1 && children[0].type().id() == LogicalTypeId::VARCHAR &&
	    children[0].GetValue<string>() == "*") {
		result.resize(names.size(), true);
		return result;
	}
	return ParseColumnList(children, names, loption);
}

vector<idx_t> ParseColumnsOrdered(const vector<Value> &set, const vector<string> &names, const string &loption) {
	vector<idx_t> result;

	if (set.empty()) {
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}

	// Maps option to bool indicating if its found and the index in the original set
	case_insensitive_map_t<std::pair<bool, idx_t>> option_map;
	for (idx_t i = 0; i < set.size(); i++) {
		option_map[set[i].ToString()] = {false, i};
	}
	result.resize(option_map.size());

	for (idx_t i = 0; i < names.size(); i++) {
		auto entry = option_map.find(names[i]);
		if (entry != option_map.end()) {
			result[entry->second.second] = i;
			entry->second.first = true;
		}
	}
	for (auto &entry : option_map) {
		if (!entry.second.first) {
			throw BinderException("\"%s\" expected to find %s, but it was not found in the table", loption,
			                      entry.first.c_str());
		}
	}
	return result;
}

vector<idx_t> ParseColumnsOrdered(const Value &value, const vector<string> &names, const string &loption) {
	vector<idx_t> result;

	// Only accept a list of arguments
	if (value.type().id() != LogicalTypeId::LIST) {
		// Support a single argument if it's '*'
		if (value.type().id() == LogicalTypeId::VARCHAR && value.GetValue<string>() == "*") {
			result.resize(names.size(), 0);
			std::iota(std::begin(result), std::end(result), 0);
			return result;
		}
		throw BinderException("\"%s\" expects a column list or * as parameter", loption);
	}
	auto &children = ListValue::GetChildren(value);
	// accept '*' as single argument
	if (children.size() == 1 && children[0].type().id() == LogicalTypeId::VARCHAR &&
	    children[0].GetValue<string>() == "*") {
		result.resize(names.size(), 0);
		std::iota(std::begin(result), std::end(result), 0);
		return result;
	}
	return ParseColumnsOrdered(children, names, loption);
}

vector<BoundOrderByNode> ParseOrderByColumns(Binder &binder, const vector<Value> &set,
                                             const BoundStatement &bound_statement, const string &loption) {
	// Parse
	vector<string> order_by_strings;
	for (auto &value : set) {
		order_by_strings.push_back(value.ToString());
	}
	const auto order_by_clause = StringUtil::Join(order_by_strings, ", ");
	auto parsed_orders = Parser::ParseOrderList(order_by_clause);

	// Bind
	auto &config = DBConfig::GetConfig(binder.context);
	auto child_binder = Binder::CreateBinder(binder.context, &binder);
	auto table_index = binder.GenerateTableIndex();
	child_binder->bind_context.AddGenericBinding(table_index, "__copy_input", bound_statement.names,
	                                             bound_statement.types);
	ExpressionBinder expr_binder(*child_binder, binder.context);
	vector<BoundOrderByNode> bound_orders;
	for (auto &parsed_order : parsed_orders) {
		const auto order_type = config.ResolveOrder(binder.context, parsed_order.type);
		const auto null_order = config.ResolveNullOrder(binder.context, order_type, parsed_order.null_order);
		bound_orders.emplace_back(order_type, null_order, expr_binder.Bind(parsed_order.expression));
	}

	// Convert BoundColumnRefExpression to BoundReferenceExpression
	vector<Value> name_values;
	case_insensitive_map_t<vector<reference<unique_ptr<Expression>>>> name_to_colref;
	for (auto &bound_order : bound_orders) {
		ExpressionIterator::VisitExpressionClassMutable(bound_order.expression, ExpressionClass::BOUND_COLUMN_REF,
		                                                [&](unique_ptr<Expression> &child) {
			                                                name_values.push_back(child->ToString());
			                                                name_to_colref[child->ToString()].push_back(child);
		                                                });
	}
	const auto indices = ParseColumnsOrdered(name_values, bound_statement.names, loption);
	D_ASSERT(name_values.size() == indices.size());
	for (idx_t i = 0; i < indices.size(); i++) {
		auto name = name_values[i].ToString();
		auto &expressions = name_to_colref[name];
		for (auto &expr : expressions) {
			expr.get() = make_uniq<BoundReferenceExpression>(name, expr.get()->GetReturnType(), indices[i]);
		}
	}

	return bound_orders;
}

} // namespace duckdb
