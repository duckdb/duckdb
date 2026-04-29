#include "duckdb/optimizer/rule/struct_extract_struct_pack_folding.hpp"

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/optimizer/matcher/function_matcher.hpp"
#include "duckdb/optimizer/matcher/set_matcher.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

StructExtractStructPackFoldingRule::StructExtractStructPackFoldingRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto func = make_uniq<FunctionExpressionMatcher>();
	// Bracket subscript on a struct uses the "array_extract" overload; both must be folded the same way.
	func->function = make_uniq<ManyFunctionMatcher>(unordered_set<string> {"struct_extract", "array_extract"});
	func->policy = SetMatcher::Policy::ORDERED;
	auto packed = make_uniq<FunctionExpressionMatcher>();
	packed->function = make_uniq<SpecificFunctionMatcher>("struct_pack");
	packed->policy = SetMatcher::Policy::SOME;
	func->matchers.push_back(std::move(packed));
	func->matchers.push_back(make_uniq<ExpressionMatcher>());
	root = std::move(func);
}

unique_ptr<Expression> StructExtractStructPackFoldingRule::Apply(LogicalOperator &op,
                                                                 vector<reference<Expression>> &bindings,
                                                                 bool &changes_made, bool is_root) {
	auto &extract = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &packed = bindings[1].get().Cast<BoundFunctionExpression>();
	if (!extract.bind_info) {
		return nullptr;
	}
	auto &struct_data = extract.bind_info->Cast<StructExtractBindData>();
	const auto field_index = struct_data.index;
	if (field_index >= packed.children.size()) {
		return nullptr;
	}
	// struct_extract(struct_pack(e0,..,en), k) is always semantically equivalent to e_i
	auto result = packed.children[field_index]->Copy();
	result = BoundCastExpression::AddCastToType(GetContext(), std::move(result), extract.function.GetReturnType());
	if (!result) {
		return nullptr;
	}
	return result;
}

} // namespace duckdb
