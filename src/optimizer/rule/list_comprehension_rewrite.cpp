#include "duckdb/optimizer/rule/list_comprehension_rewrite.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/function/lambda_functions.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
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
	if (struct_pack.return_type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(struct_pack.return_type);
		for (idx_t i = 0; i < child_types.size(); i++) {
			if (StringUtil::CIEquals(child_types[i].first, name)) {
				return struct_pack.children[i].get();
			}
		}
	}
	for (auto &child : struct_pack.children) {
		if (StringUtil::CIEquals(child->GetAlias(), name)) {
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

} // namespace

ListComprehensionRewriteRule::ListComprehensionRewriteRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	root = make_uniq<ExpressionMatcher>(ExpressionClass::BOUND_FUNCTION);
}

unique_ptr<Expression> ListComprehensionRewriteRule::Apply(LogicalOperator &, vector<reference<Expression>> &bindings,
                                                           bool &, bool) {
	auto &root = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &context = GetContext();
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

	// Build list_filter(list, lambda filter_expr)
	vector<unique_ptr<Expression>> filter_children;
	filter_children.reserve(inner_apply.children.size());
	for (idx_t i = 0; i < inner_apply.children.size(); i++) {
		filter_children.push_back(std::move(inner_apply.children[i]));
	}

	auto filter_return_type = filter_children[0]->return_type;
	auto filter_bind_info =
	    make_uniq<ListLambdaBindData>(filter_return_type, filter_expr->Copy(), inner_bind.has_index);
	auto new_filter =
	    make_uniq<BoundFunctionExpression>(filter_return_type, list_filter_expr.function, std::move(filter_children),
	                                       std::move(filter_bind_info), list_filter_expr.is_operator);

	// Build list_apply(list_filter(...), lambda result_expr)
	vector<unique_ptr<Expression>> apply_children;
	apply_children.reserve(inner_apply.children.size());
	apply_children.push_back(std::move(new_filter));
	for (idx_t i = 1; i < inner_apply.children.size(); i++) {
		apply_children.push_back(inner_apply.children[i]->Copy());
	}

	auto apply_return_type = LogicalType::LIST(result_expr->return_type);
	auto apply_bind_info = make_uniq<ListLambdaBindData>(apply_return_type, result_expr->Copy(), inner_bind.has_index);
	auto new_apply = make_uniq<BoundFunctionExpression>(apply_return_type, root.function, std::move(apply_children),
	                                                    std::move(apply_bind_info), root.is_operator);
	return std::move(new_apply);
}

} // namespace duckdb
