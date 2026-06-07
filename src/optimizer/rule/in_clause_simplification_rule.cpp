
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/optimizer/rule/in_clause_simplification.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

InClauseSimplificationRule::InClauseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on InClauseExpression that has a ConstantExpression as a check
	auto op = make_uniq<InClauseExpressionMatcher>();
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

unique_ptr<Expression> InClauseSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                         bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundOperatorExpression>();
	if (expr.GetChildrenMutable()[0]->GetExpressionClass() != ExpressionClass::BOUND_CAST) {
		return nullptr;
	}
	auto &cast_expression = expr.GetChildrenMutable()[0]->Cast<BoundCastExpression>();
	if (cast_expression.Child().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	//! The goal here is to remove the cast from the probe expression
	//! and apply a cast to the constant expressions. We can only do this
	//! if the semantics do not change, which only happens when BOTH casts
	//! are invertible.
	auto target_type = cast_expression.source_type();
	if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression.GetReturnType())) {
		return nullptr;
	}
	vector<unique_ptr<BoundConstantExpression>> cast_list;
	//! First check if we can cast all children
	for (size_t i = 1; i < expr.GetChildrenMutable().size(); i++) {
		if (expr.GetChildrenMutable()[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
			return nullptr;
		}
		D_ASSERT(expr.GetChildrenMutable()[i]->IsFoldable());
		auto constant_value = ExpressionExecutor::EvaluateScalar(GetContext(), *expr.GetChildrenMutable()[i]);
		if (!BoundCastExpression::CastIsInvertible(constant_value.type(), target_type)) {
			return nullptr;
		}
		auto new_constant = constant_value.DefaultTryCastAs(target_type);
		if (!new_constant) {
			return nullptr;
		} else {
			auto new_constant_expr = make_uniq<BoundConstantExpression>(constant_value);
			cast_list.push_back(std::move(new_constant_expr));
		}
	}
	//! We can cast, so we move the new constant
	for (size_t i = 1; i < expr.GetChildrenMutable().size(); i++) {
		expr.GetChildrenMutable()[i] = std::move(cast_list[i - 1]);

		//		expr->children[i] = std::move(new_constant_expr);
	}
	//! We can cast the full list, so we move the column
	expr.GetChildrenMutable()[0] = std::move(cast_expression.ChildMutable());
	return nullptr;
}

InEnumSimplificationRule::InEnumSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match enum::VARCHAR IN (string literals)
	auto op = make_uniq<InUniformExpressionMatcher>();

	//	Probe must be enum::VARCHAR
	auto cast_matcher = make_uniq<CastExpressionMatcher>();
	cast_matcher->type = make_uniq<SpecificTypeMatcher>(LogicalType::VARCHAR);
	auto enum_matcher = make_uniq<ExpressionMatcher>();
	enum_matcher->type = make_uniq<SpecificTypeIdMatcher>(LogicalTypeId::ENUM);
	cast_matcher->matcher = std::move(enum_matcher);
	op->probe_matcher = std::move(cast_matcher);

	//	Children must be constant strings
	vector<LogicalTypeId> ids;
	ids.emplace_back(LogicalTypeId::VARCHAR);
	ids.emplace_back(LogicalTypeId::STRING_LITERAL);
	ids.emplace_back(LogicalTypeId::SQLNULL);
	op->child_matcher = make_uniq<ExpressionMatcher>(ExpressionClass::BOUND_CONSTANT);
	op->child_matcher->type = make_uniq<SpecificTypeMatcher>(LogicalType::VARCHAR);

	root = std::move(op);
}

unique_ptr<Expression> InEnumSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                       bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundOperatorExpression>();
	auto &children = expr.GetChildrenMutable();
	auto &cast_expr = children[0]->Cast<BoundCastExpression>();
	auto &enum_expr = cast_expr.Child();
	auto &enum_type = enum_expr.GetReturnType();

	// Look up the domain for the original ENUM
	string_map_t<uint64_t> enum_domain;
	auto strings = FlatVector::GetData<string_t>(EnumType::GetValuesInsertOrder(enum_type));
	for (uint64_t i = 0; i < EnumType::GetSize(enum_type); ++i) {
		enum_domain[strings[i]] = i;
	}

	vector<unique_ptr<Expression>> in_children;
	in_children.emplace_back(enum_expr.Copy());

	for (idx_t i = 1; i < children.size(); ++i) {
		auto &child = *children[i];
		auto &v = child.Cast<BoundConstantExpression>().GetValue();
		if (v.IsNull()) {
			//	Keep NULLs
			in_children.emplace_back(make_uniq<BoundConstantExpression>(Value(enum_type)));
		} else {
			auto s = v.ToString();
			auto it = enum_domain.find(s);
			if (it != enum_domain.end()) {
				//	The literal is in the ENUM domain, so translate it
				in_children.emplace_back(make_uniq<BoundConstantExpression>(Value::ENUM(it->second, enum_type)));
			}
			// Not in the domain, so ignore it.
		}
	}

	//	If ALL the children are string constants being matched against an ENUM (NOT) IN,
	//	and SOME of them are in the domain
	//	then swap out the children for the valid ENUM values
	if (in_children.size() > 1) {
		children.swap(in_children);
	} else {
		// constant_or_null(not_in, child[0])
		const bool not_in = (expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN);
		return rewriter.ConstantOrNull(in_children[0]->Copy(), Value::BOOLEAN(not_in));
	}

	return nullptr;
}

} // namespace duckdb
