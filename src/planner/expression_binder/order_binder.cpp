#include "duckdb/planner/expression_binder/order_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/expression_binder.hpp"

using namespace std;

namespace duckdb {

OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, unordered_map<string, idx_t> &alias_map,
                         expression_map_t<idx_t> &projection_map, idx_t max_count)
    : binders(move(binders)), projection_index(projection_index), max_count(max_count), extra_list(nullptr),
      alias_map(alias_map), projection_map(projection_map) {
}
OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, SelectNode &node,
                         unordered_map<string, idx_t> &alias_map, expression_map_t<idx_t> &projection_map)
    : binders(move(binders)), projection_index(projection_index), alias_map(alias_map), projection_map(projection_map) {
	this->max_count = node.select_list.size();
	this->extra_list = &node.select_list;
}

unique_ptr<Expression> OrderBinder::CreateProjectionReference(ParsedExpression &expr, idx_t index) {
	return make_unique<BoundColumnRefExpression>(expr.GetName(), TypeId::INVALID,
	                                             ColumnBinding(projection_index, index));
}

unique_ptr<Expression> OrderBinder::Bind(unique_ptr<ParsedExpression> expr) {
	// in the ORDER BY clause we do not bind children
	// we bind ONLY to the select list
	// if there is no matching entry in the SELECT list already, we add the expression to the SELECT list and refer the
	// new expression the new entry will then be bound later during the binding of the SELECT list we also don't do type
	// resolution here: this only happens after the SELECT list has been bound
	switch (expr->expression_class) {
	case ExpressionClass::CONSTANT: {
		// ORDER BY constant
		// is the ORDER BY expression a constant integer? (e.g. ORDER BY 1)
		auto &constant = (ConstantExpression &)*expr;
		// ORDER BY a constant
		if (!TypeIsIntegral(constant.value.type)) {
			// non-integral expression, we just leave the constant here.
			// ORDER BY <constant> has no effect
			// CONTROVERSIAL: maybe we should throw an error
			return nullptr;
		}
		// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
		auto index = (idx_t)constant.value.GetValue<int64_t>();
		if (index < 1 || index > max_count) {
			throw BinderException("ORDER term out of range - should be between 1 and %lld", (idx_t)max_count);
		}
		return CreateProjectionReference(*expr, index - 1);
	}
	case ExpressionClass::COLUMN_REF: {
		// COLUMN REF expression
		// check if we can bind it to an alias in the select list
		auto &colref = (ColumnRefExpression &)*expr;
		// if there is an explicit table name we can't bind to an alias
		if (!colref.table_name.empty()) {
			break;
		}
		// check the alias list
		auto entry = alias_map.find(colref.column_name);
		if (entry != alias_map.end()) {
			// it does! point it to that entry
			return CreateProjectionReference(*expr, entry->second);
		}
		break;
	}
	default:
		break;
	}
	// general case
	// first bind the table names of this entry
	for (auto &binder : binders) {
		ExpressionBinder::BindTableNames(*binder, *expr);
	}
	// first check if the ORDER BY clause already points to an entry in the projection list
	auto entry = projection_map.find(expr.get());
	if (entry != projection_map.end()) {
		if (entry->second == INVALID_INDEX) {
			throw BinderException("Ambiguous reference to column");
		}
		// there is a matching entry in the projection list
		// just point to that entry
		return CreateProjectionReference(*expr, entry->second);
	}
	if (!extra_list) {
		// no extra list specified: we cannot push an extra ORDER BY clause
		throw BinderException("Could not ORDER BY column \"%s\": add the expression/function to every SELECT, or move "
		                      "the UNION into a FROM clause.",
		                      expr->ToString().c_str());
	}
	// otherwise we need to push the ORDER BY entry into the select list
	auto result = CreateProjectionReference(*expr, extra_list->size());
	extra_list->push_back(move(expr));
	return result;
}

} // namespace duckdb
