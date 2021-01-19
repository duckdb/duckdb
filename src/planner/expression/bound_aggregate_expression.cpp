#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

BoundAggregateExpression::BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
                                                   unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info,
                                                   bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, function.return_type),
      function(move(function)), children(move(children)), bind_info(move(bind_info)), distinct(distinct),
      filter(move(filter)) {
}

string BoundAggregateExpression::ToString() const {
	string result = function.name + "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	result += StringUtil::Join(children, children.size(), ", ",
	                           [](const unique_ptr<Expression> &child) { return child->ToString(); });
	result += ")";
	return result;
}

hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, function.Hash());
	result = CombineHash(result, duckdb::Hash(distinct));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_;
	if (other->distinct != distinct) {
		return false;
	}
	if (other->function != function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	if (other->filter != filter) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	if (!FunctionData::Equals(bind_info.get(), other->bind_info.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundAggregateExpression::ExtractColumnRef(unique_ptr<Expression> filter,
                                                                  vector<unique_ptr<Expression>> &expressions,
                                                                  vector<LogicalType> &types) {
	switch (filter->expression_class) {
	case ExpressionClass::BOUND_REF: {
		auto &column_ref = (BoundReferenceExpression &)*filter;
		auto ref = make_unique<BoundReferenceExpression>(column_ref.return_type, expressions.size());
		for (size_t i = 0; i < expressions.size(); i++){
			auto *base_expr = (BaseExpression *)expressions[i].get();
			if (column_ref.Equals(base_expr)) {
				ref->index = i;
				filter = move(ref);
				return filter;
			}
		}
		types.push_back(column_ref.return_type);
		expressions.push_back(move(filter));
		filter = move(ref);
		return filter;
	}
	case ExpressionClass::BOUND_CONSTANT:
		return filter;
	case ExpressionClass::BOUND_CAST: {
		auto &cast = (BoundCastExpression &)*filter;
		cast.child = ExtractColumnRef(move(cast.child), expressions, types);
		return filter;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = (BoundComparisonExpression &)*filter;
		comparison.left = ExtractColumnRef(move(comparison.left), expressions, types);
		comparison.right = ExtractColumnRef(move(comparison.right), expressions, types);
		return filter;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = (BoundConjunctionExpression &)*filter;
		for (auto &child : conj.children) {
			child = ExtractColumnRef(move(child), expressions, types);
		}
		return filter;
	}
	default:
		throw NotImplementedException("Filter not implemented within aggregation");
	}
}

void BoundAggregateExpression::GetColumnRef(Expression* filter,vector<vector<Expression*>> &expressions,vector<LogicalType> &types) {
	switch (filter->expression_class) {
	case ExpressionClass::BOUND_REF: {
		auto &ref = (BoundReferenceExpression &)*filter;
		for (auto &expr : expressions) {
			auto *base_expr = (BaseExpression *)expr[0];
			if (ref.Equals(base_expr)) {
				expr.push_back(&ref);
				return;
			}
		}
		types.push_back(ref.return_type);
		expressions.push_back({&ref});
		break;
	}
	case ExpressionClass::BOUND_CONSTANT:
		break;
	case ExpressionClass::BOUND_CAST: {
		auto &cast = (BoundCastExpression &)*filter;
		GetColumnRef(cast.child.get(), expressions, types);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = (BoundComparisonExpression &)*filter;
		GetColumnRef(comparison.left.get(), expressions, types);
		GetColumnRef(comparison.right.get(), expressions, types);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = (BoundConjunctionExpression &)*filter;
		for (auto &child : conj.children) {
			GetColumnRef(child.get(), expressions, types);
		}
		break;
	}
	default:
		throw NotImplementedException("Filter not implemented within aggregation");
	}
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info->Copy();
	auto new_filter = filter->Copy();
	auto copy = make_unique<BoundAggregateExpression>(function, move(new_children), move(new_filter),
	                                                  move(new_bind_info), distinct);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
