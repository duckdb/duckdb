#include "duckdb/optimizer/rule/list_comprehension_rewrite.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

namespace {

bool IsTargetListFunction(ClientContext &context, const BoundFunctionExpression &expr, const string &target_name) {
	// Compare function name with catalog to recognize aliases
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto entry = catalog.GetEntry<ScalarFunctionCatalogEntry>(context, DEFAULT_SCHEMA, expr.function.name,
	                                                          OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return false;
	}
	if (!entry->alias_of.empty()) {
		return entry->alias_of == target_name;
	}
	return entry->name == target_name;
}

bool IsStructPack(const BoundFunctionExpression &expr) {
	return expr.function.name == "struct_pack";
}

optional_ptr<Expression> UnwrapCasts(optional_ptr<Expression> expr) {
	while (expr->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast_expr = expr->Cast<BoundCastExpression>();
		expr = cast_expr.child.get();
	}
	return expr;
}

optional_ptr<Expression> FindStructPackChildByName(BoundFunctionExpression &struct_pack, const string &name) {
	D_ASSERT(struct_pack.return_type.id() == LogicalTypeId::STRUCT);
	for (auto &child : struct_pack.children) {
		if (child->GetAlias() == name) {
			return child.get();
		}
	}
	return nullptr;
}

bool UsesIndexParameter(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto &ref = expr.Cast<BoundReferenceExpression>();
		if (ref.index == 1) {
			return true;
		}
	}
	bool uses_index = false;
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		if (uses_index) {
			return;
		}
		if (child->GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = child->Cast<BoundReferenceExpression>();
			if (ref.index == 1) {
				uses_index = true;
				return;
			}
		}
		if (!uses_index) {
			uses_index = UsesIndexParameter(*child);
		}
	});
	return uses_index;
}

bool MatchesStructFieldProjection(Expression &expr, const string &field_name) {
	auto base = UnwrapCasts(expr);
	if (base->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &extract_expr = base->Cast<BoundFunctionExpression>();
	if (extract_expr.function.name != "struct_extract" || extract_expr.children.size() != 2) {
		return false;
	}

	auto struct_arg = UnwrapCasts(*extract_expr.children[0]);
	if (struct_arg->GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	auto &struct_ref = struct_arg->Cast<BoundReferenceExpression>();
	if (struct_ref.index != 0) {
		return false;
	}

	auto field_arg = UnwrapCasts(*extract_expr.children[1]);
	if (field_arg->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &field_constant = field_arg->Cast<BoundConstantExpression>();
	if (field_constant.value.IsNull() || field_constant.value.type() != LogicalTypeId::VARCHAR) {
		return false;
	}

	return StringValue::Get(field_constant.value) == field_name;
}

struct ListComprehensionMatch {
	optional_ptr<BoundFunctionExpression> root;
	optional_ptr<BoundFunctionExpression> list_filter_expr;
	optional_ptr<BoundFunctionExpression> inner_apply;
	optional_ptr<ListLambdaBindData> inner_bind;
	optional_ptr<Expression> filter_expr;
	optional_ptr<Expression> result_expr;
	vector<unique_ptr<Expression>> captured_children;
};

vector<unique_ptr<Expression>> CopyCapturedChildren(BoundFunctionExpression &inner_apply) {
	vector<unique_ptr<Expression>> captured_children;
	captured_children.reserve(inner_apply.children.empty() ? 0 : inner_apply.children.size() - 1);
	for (idx_t i = 1; i < inner_apply.children.size(); i++) {
		captured_children.push_back(inner_apply.children[i]->Copy());
	}
	return captured_children;
}

unique_ptr<ListComprehensionMatch> MatchListComprehensionRewrite(ClientContext &context,
                                                                 BoundFunctionExpression &root) {
	// Check if expression has a list_transform -> list_filter -> list_transform pattern from list comprehension
	if (!IsTargetListFunction(context, root, "list_transform")) {
		return nullptr;
	}
	if (root.children.empty()) {
		return nullptr;
	}
	if (root.children[0]->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &list_filter_expr = root.children[0]->Cast<BoundFunctionExpression>();
	if (!IsTargetListFunction(context, list_filter_expr, "list_filter")) {
		return nullptr;
	}
	if (!list_filter_expr.bind_info || !root.bind_info) {
		return nullptr;
	}
	auto &list_filter_bind = list_filter_expr.bind_info->Cast<ListLambdaBindData>();
	auto &root_bind = root.bind_info->Cast<ListLambdaBindData>();
	if (!list_filter_bind.lambda_expr || !root_bind.lambda_expr) {
		return nullptr;
	}
	if (!MatchesStructFieldProjection(*list_filter_bind.lambda_expr, "filter") ||
	    !MatchesStructFieldProjection(*root_bind.lambda_expr, "result")) {
		return nullptr;
	}
	if (list_filter_expr.children.empty()) {
		return nullptr;
	}
	if (list_filter_expr.children[0]->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &inner_apply = list_filter_expr.children[0]->Cast<BoundFunctionExpression>();
	if (!IsTargetListFunction(context, inner_apply, "list_transform") || !inner_apply.bind_info) {
		return nullptr;
	}
	auto &inner_bind = inner_apply.bind_info->Cast<ListLambdaBindData>();
	if (!inner_bind.lambda_expr) {
		return nullptr;
	}
	auto inner_base = UnwrapCasts(*inner_bind.lambda_expr);
	if (inner_base->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &struct_pack = inner_base->Cast<BoundFunctionExpression>();
	if (!IsStructPack(struct_pack)) {
		return nullptr;
	}
	auto filter_expr = FindStructPackChildByName(struct_pack, "filter");
	auto result_expr = FindStructPackChildByName(struct_pack, "result");
	if (!filter_expr || !result_expr) {
		return nullptr;
	}
	if (inner_bind.has_index && UsesIndexParameter(*result_expr)) {
		return nullptr;
	}
	if (result_expr->IsVolatile()) {
		return nullptr;
	}

	auto match = make_uniq<ListComprehensionMatch>();
	match->root = root;
	match->list_filter_expr = list_filter_expr;
	match->inner_apply = inner_apply;
	match->inner_bind = inner_bind;
	match->filter_expr = filter_expr;
	match->result_expr = result_expr;
	match->captured_children = CopyCapturedChildren(inner_apply);
	return match;
}

unique_ptr<Expression> BuildListComprehensionRewrite(ListComprehensionMatch &match) {
	auto &inner_apply = *match.inner_apply;
	auto &list_filter_expr = *match.list_filter_expr;
	auto &root = *match.root;
	auto &inner_bind = *match.inner_bind;
	auto &filter_expr = *match.filter_expr;
	auto &result_expr = *match.result_expr;

	// Build list_filter(list, lambda filter_expr)
	vector<unique_ptr<Expression>> filter_children;
	filter_children.reserve(inner_apply.children.size());
	for (idx_t i = 0; i < inner_apply.children.size(); i++) {
		filter_children.push_back(std::move(inner_apply.children[i]));
	}

	auto filter_return_type = filter_children[0]->return_type;
	auto filter_bind_info = make_uniq<ListLambdaBindData>(filter_return_type, filter_expr.Copy(), inner_bind.has_index);
	auto new_filter =
	    make_uniq<BoundFunctionExpression>(filter_return_type, list_filter_expr.function, std::move(filter_children),
	                                       std::move(filter_bind_info), list_filter_expr.is_operator);

	// Build list_apply(list_filter(...), lambda result_expr)
	vector<unique_ptr<Expression>> apply_children;
	apply_children.reserve(inner_apply.children.size());
	apply_children.push_back(std::move(new_filter));
	for (auto &captured_child : match.captured_children) {
		apply_children.push_back(std::move(captured_child));
	}

	auto apply_return_type = LogicalType::LIST(result_expr.return_type);
	auto apply_bind_info = make_uniq<ListLambdaBindData>(apply_return_type, result_expr.Copy(), inner_bind.has_index);
	return make_uniq<BoundFunctionExpression>(apply_return_type, root.function, std::move(apply_children),
	                                          std::move(apply_bind_info), root.is_operator);
}

} // namespace

ListComprehensionRewriteRule::ListComprehensionRewriteRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>(ExpressionClass::BOUND_FUNCTION);
}

unique_ptr<Expression> ListComprehensionRewriteRule::Apply(LogicalOperator &, vector<reference<Expression>> &bindings,
                                                           bool &, bool) {
	auto &root = bindings[0].get().Cast<BoundFunctionExpression>();
	auto match = MatchListComprehensionRewrite(GetContext(), root);
	if (!match) {
		return nullptr;
	}
	return BuildListComprehensionRewrite(*match);
}

} // namespace duckdb
