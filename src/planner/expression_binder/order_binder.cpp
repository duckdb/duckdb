#include "duckdb/planner/expression_binder/order_binder.hpp"

#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

OrderBinder::OrderBinder(vector<Binder *> binders, SelectBindState &bind_state)
    : binders(std::move(binders)), extra_list(nullptr), bind_state(bind_state) {
}
OrderBinder::OrderBinder(vector<Binder *> binders, SelectNode &node, SelectBindState &bind_state)
    : binders(std::move(binders)), bind_state(bind_state) {
	this->extra_list = &node.select_list;
}

unique_ptr<Expression> OrderBinder::CreateProjectionReference(ParsedExpression &expr, const idx_t index) {
	string alias;
	if (extra_list && index < extra_list->size()) {
		alias = extra_list->at(index)->ToString();
	} else {
		if (!expr.alias.empty()) {
			alias = expr.alias;
		}
	}
	auto result = make_uniq<BoundConstantExpression>(Value::UBIGINT(index));
	result->alias = std::move(alias);
	result->query_location = expr.query_location;
	return std::move(result);
}

unique_ptr<Expression> OrderBinder::CreateExtraReference(unique_ptr<ParsedExpression> expr) {
	if (!extra_list) {
		throw InternalException("CreateExtraReference called without extra_list");
	}
	bind_state.projection_map[*expr] = extra_list->size();
	auto result = CreateProjectionReference(*expr, extra_list->size());
	extra_list->push_back(std::move(expr));
	return result;
}

unique_ptr<Expression> OrderBinder::BindConstant(ParsedExpression &expr, const Value &val) {
	// ORDER BY a constant
	if (!val.type().IsIntegral()) {
		// non-integral expression, we just leave the constant here.
		// ORDER BY <constant> has no effect
		// CONTROVERSIAL: maybe we should throw an error
		return nullptr;
	}
	// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
	auto order_value = val.GetValue<int64_t>();
	auto index = order_value <= 0 ? NumericLimits<idx_t>::Maximum() : idx_t(order_value - 1);
	child_list_t<Value> values;
	values.push_back(make_pair("index", Value::UBIGINT(index)));
	auto result = make_uniq<BoundConstantExpression>(Value::STRUCT(std::move(values)));
	result->alias = std::move(expr.alias);
	result->query_location = expr.query_location;
	return std::move(result);
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
		auto &constant = expr->Cast<ConstantExpression>();
		return BindConstant(*expr, constant.value);
	}
	case ExpressionClass::COLUMN_REF: {
		// COLUMN REF expression
		// check if we can bind it to an alias in the select list
		auto &colref = expr->Cast<ColumnRefExpression>();
		// if there is an explicit table name we can't bind to an alias
		if (colref.IsQualified()) {
			break;
		}
		// check the alias list
		auto entry = bind_state.alias_map.find(colref.column_names[0]);
		if (entry != bind_state.alias_map.end()) {
			// it does! point it to that entry
			return CreateProjectionReference(*expr, entry->second);
		}
		break;
	}
	case ExpressionClass::POSITIONAL_REFERENCE: {
		auto &posref = expr->Cast<PositionalReferenceExpression>();
		return CreateProjectionReference(*expr, posref.index - 1);
	}
	case ExpressionClass::PARAMETER: {
		throw ParameterNotAllowedException("Parameter not supported in ORDER BY clause");
	}
	case ExpressionClass::COLLATE: {
		auto &collation = expr->Cast<CollateExpression>();
		if (collation.child->expression_class == ExpressionClass::CONSTANT) {
			auto &constant = collation.child->Cast<ConstantExpression>();

			// non-integral expression, we just leave the constant here.
			// ORDER BY <constant> has no effect
			// CONTROVERSIAL: maybe we should throw an error
			if (!constant.value.type().IsIntegral()) {
				return nullptr;
			}

			D_ASSERT(constant.value.GetValue<idx_t>() > 0);
			auto index = constant.value.GetValue<idx_t>() - 1;
			child_list_t<Value> values;
			values.push_back(make_pair("index", Value::UBIGINT(index)));
			values.push_back(make_pair("collation", Value(std::move(collation.collation))));
			return make_uniq<BoundConstantExpression>(Value::STRUCT(std::move(values)));
		}
		break;
	}
	default:
		break;
	}
	// general case
	// first bind the table names of this entry
	for (auto &binder : binders) {
		ExpressionBinder::QualifyColumnNames(*binder, expr);
	}
	// first check if the ORDER BY clause already points to an entry in the projection list
	auto entry = bind_state.projection_map.find(*expr);
	if (entry != bind_state.projection_map.end()) {
		if (entry->second == DConstants::INVALID_INDEX) {
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
		                      expr->ToString());
	}
	// otherwise we need to push the ORDER BY entry into the select list
	return CreateExtraReference(std::move(expr));
}

} // namespace duckdb
