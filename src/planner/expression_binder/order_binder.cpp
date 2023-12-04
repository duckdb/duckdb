#include "duckdb/planner/expression_binder/order_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, case_insensitive_map_t<idx_t> &alias_map,
                         parsed_expression_map_t<idx_t> &projection_map, idx_t max_count)
    : binders(std::move(binders)), projection_index(projection_index), max_count(max_count), extra_list(nullptr),
      alias_map(alias_map), projection_map(projection_map) {
}
OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, SelectNode &node,
                         case_insensitive_map_t<idx_t> &alias_map, parsed_expression_map_t<idx_t> &projection_map)
    : binders(std::move(binders)), projection_index(projection_index), alias_map(alias_map),
      projection_map(projection_map) {
	this->max_count = node.select_list.size();
	this->extra_list = &node.select_list;
}

unique_ptr<Expression> OrderBinder::CreateProjectionReference(ParsedExpression &expr, idx_t index) {
	string alias;
	if (extra_list && index < extra_list->size()) {
		alias = extra_list->at(index)->ToString();
	} else {
		if (!expr.alias.empty()) {
			alias = expr.alias;
		}
	}
	return make_uniq<BoundColumnRefExpression>(std::move(alias), LogicalType::INVALID,
	                                           ColumnBinding(projection_index, index));
}

unique_ptr<Expression> OrderBinder::CreateExtraReference(unique_ptr<ParsedExpression> expr) {
	if (!extra_list) {
		throw InternalException("CreateExtraReference called without extra_list");
	}
	projection_map[*expr] = extra_list->size();
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
	auto index = (idx_t)val.GetValue<int64_t>();
	if (index < 1 || index > max_count) {
		throw BinderException("ORDER term out of range - should be between 1 and %lld", (idx_t)max_count);
	}
	return CreateProjectionReference(expr, index - 1);
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
		auto entry = alias_map.find(colref.column_names[0]);
		if (entry != alias_map.end()) {
			// it does! point it to that entry
			return CreateProjectionReference(*expr, entry->second);
		}
		break;
	}
	case ExpressionClass::POSITIONAL_REFERENCE: {
		auto &posref = expr->Cast<PositionalReferenceExpression>();
		if (posref.index < 1 || posref.index > max_count) {
			throw BinderException("ORDER term out of range - should be between 1 and %lld", (idx_t)max_count);
		}
		return CreateProjectionReference(*expr, posref.index - 1);
	}
	case ExpressionClass::PARAMETER: {
		throw ParameterNotAllowedException("Parameter not supported in ORDER BY clause");
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
	auto entry = projection_map.find(*expr);
	if (entry != projection_map.end()) {
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
