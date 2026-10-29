#include "duckdb/planner/expression_binder/case_compatibility.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_sources.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include <algorithm>

namespace duckdb {

static void VisitCaseChildren(ParsedExpression &expr,
                              const std::function<void(unique_ptr<ParsedExpression> &)> &visit) {
	if (expr.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &query = expr.Cast<SubqueryExpression>().Subquery();
		if (query) {
			ParsedExpressionIterator::EnumerateQueryNodeChildren(*query->node, visit, [](TableRef &) {}, true);
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::STAR) {
		auto &star = expr.Cast<StarExpression>();
		vector<Identifier> names;
		for (auto &entry : star.ReplaceList()) {
			names.push_back(entry.first);
		}
		std::sort(names.begin(), names.end());
		for (auto &name : names) {
			visit(star.ReplaceListMutable().at(name));
		}
		if (star.Expression()) {
			visit(star.ExpressionMutable());
		}
		return;
	}
	for (auto &child : expr.ChildrenMutable()) {
		visit(child);
	}
}

void CaseCompatibilityContext::Prepare(ParsedExpression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::CASE) {
		auto &info = expr.Cast<CaseExpression>().BindingInfoMutable();
		if (!info || info->context.lock().get() != this) {
			info = make_shared_ptr<CaseBindingInfo>();
			info->context = shared_from_this();
		}
	}
	VisitCaseChildren(expr, [&](unique_ptr<ParsedExpression> &child) { Prepare(*child); });
}

bool CaseCompatibilityContext::CanRepeatOperand(const Expression &expr, const Binder &binder) {
	if (expr.IsVolatile()) {
		return false;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto source = binder.GetExpressionSource(expr.Cast<BoundColumnRefExpression>().Binding());
		return !source.expression || source.kind == ExpressionSourceKind::GROUP ||
		       CanRepeatOperand(*source.expression, binder);
	}
	bool repeatable = true;
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto &query = expr.Cast<BoundSubqueryExpression>().Subquery();
		std::function<void(const LogicalOperator &)> visit = [&](const LogicalOperator &op) {
			LogicalOperatorVisitor::EnumerateExpressions(op, [&](optional_ptr<const unique_ptr<Expression>> child) {
				repeatable &= CanRepeatOperand(**child, binder);
			});
			for (auto &child : op.children) {
				visit(*child);
			}
		};
		if (query.plan) {
			visit(*query.plan);
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { repeatable &= CanRepeatOperand(child, binder); });
	return repeatable;
}

bool CaseCompatibilityContext::CanRepeatComparisonOperand(const Expression &expr, const Binder &binder) {
	if (expr.GetExpressionType() == ExpressionType::COMPARE_EQUAL) {
		return CanRepeatOperand(BoundComparisonExpression::Left(expr.Cast<BoundFunctionExpression>()), binder);
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto source = binder.GetExpressionSource(expr.Cast<BoundColumnRefExpression>().Binding());
		return source.expression && CanRepeatComparisonOperand(*source.expression, binder);
	}
	return false;
}

void CaseCompatibilityContext::Record(CaseExpression &expr, bool safe) {
	if (expr.BindingInfo()) {
		expr.BindingInfo()->safety = safe ? CaseBindingSafety::SAFE : CaseBindingSafety::UNSAFE;
	}
}

void CaseCompatibilityKey::Normalize(unique_ptr<ParsedExpression> &expr, vector<CaseSite> &sites,
                                     optional_ptr<vector<shared_ptr<CaseBindingInfo>>> bindings) {
	if (expr->GetExpressionClass() == ExpressionClass::CASE) {
		auto &case_expr = expr->Cast<CaseExpression>();
		auto info = case_expr.BindingInfo();
		sites.push_back({bool(case_expr.CaseOperand()), info ? info->safety : CaseBindingSafety::UNKNOWN});
		if (bindings) {
			bindings->push_back(info);
		}
		case_expr.BindingInfoMutable().reset();
		if (case_expr.CaseOperand()) {
			expr = case_expr.GetLegacyCaseExpression();
		}
	}
	VisitCaseChildren(*expr, [&](unique_ptr<ParsedExpression> &child) { Normalize(child, sites, bindings); });
}

CaseCompatibilityKey::CaseCompatibilityKey(const ParsedExpression &expr) : expression(expr.Copy()) {
	Normalize(expression, sites);
}

bool CaseCompatibilityKey::Matches(const CaseCompatibilityKey &other) const {
	if (!expression->Equals(*other.expression)) {
		return false;
	}
	D_ASSERT(sites.size() == other.sites.size());
	for (idx_t i = 0; i < sites.size(); i++) {
		if (sites[i].simple != other.sites[i].simple && sites[i].safety != CaseBindingSafety::SAFE) {
			return false;
		}
	}
	return true;
}

void CaseCompatibilityKey::TransferTo(const ParsedExpression &expr) const {
	if (CaseExpression::ContainsLambdaReference(expr)) {
		return;
	}
	auto copy = expr.Copy();
	vector<CaseSite> target_sites;
	vector<shared_ptr<CaseBindingInfo>> bindings;
	Normalize(copy, target_sites, bindings);
	if (!expression->Equals(*copy)) {
		// A group alias has no CASE sites to transfer.
		return;
	}
	D_ASSERT(sites.size() == bindings.size());
	for (idx_t i = 0; i < sites.size(); i++) {
		if (bindings[i]) {
			bindings[i]->safety = sites[i].safety;
		}
	}
}
} // namespace duckdb
