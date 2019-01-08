#include "optimizer/obsolete_filter_rewriter.hpp"

#include "planner/operator/logical_filter.hpp"

using namespace duckdb;
using namespace std;

class RewriteSubqueries : public SQLNodeVisitor {
public:
	using SQLNodeVisitor::Visit;
	unique_ptr<Expression> Visit(SubqueryExpression &subquery) override {
		// we perform join reordering within the subquery expression
		ObsoleteFilterRewriter rewriter;
		subquery.op = rewriter.Rewrite(move(subquery.op));
		return nullptr;
	}
};

struct ExpressionValueInformation {
	size_t expression_index;
	Value constant;
	ExpressionType comparison_type;
};

enum class ValueComparisonResult { PRUNE_LEFT, PRUNE_RIGHT, UNSATISFIABLE_CONDITION, PRUNE_NOTHING };

static bool IsGreaterThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_GREATERTHAN || type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
}

static bool IsLessThan(ExpressionType type) {
	return type == ExpressionType::COMPARE_LESSTHAN || type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
}

ValueComparisonResult InvertValueComparisonResult(ValueComparisonResult result) {
	if (result == ValueComparisonResult::PRUNE_RIGHT) {
		return ValueComparisonResult::PRUNE_LEFT;
	}
	if (result == ValueComparisonResult::PRUNE_LEFT) {
		return ValueComparisonResult::PRUNE_RIGHT;
	}
	return result;
}

ValueComparisonResult CompareValueInformation(ExpressionValueInformation &left, ExpressionValueInformation &right) {
	if (left.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// left is COMPARE_EQUAL, we can either
		// (1) prune the right side or
		// (2) return UNSATISFIABLE
		bool prune_right_side = false;
		switch (right.comparison_type) {
		case ExpressionType::COMPARE_LESSTHAN:
			prune_right_side = left.constant < right.constant;
			break;
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			prune_right_side = left.constant <= right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHAN:
			prune_right_side = left.constant > right.constant;
			break;
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			prune_right_side = left.constant >= right.constant;
			break;
		default:
			assert(right.comparison_type == ExpressionType::COMPARE_EQUAL);
			prune_right_side = left.constant == right.constant;
			break;
		}
		if (prune_right_side) {
			return ValueComparisonResult::PRUNE_RIGHT;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else if (right.comparison_type == ExpressionType::COMPARE_EQUAL) {
		// right is COMPARE_EQUAL
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	} else if (IsGreaterThan(left.comparison_type) && IsGreaterThan(right.comparison_type)) {
		// both comparisons are [>], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant > right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant < right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [>=] and the other is [>]
			// we want to prune the [>=] because [>] is more selective
			// if left is [>=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type) && IsLessThan(right.comparison_type)) {
		// both comparisons are [<], we can either
		// (1) prune the left side or
		// (2) prune the right side
		if (left.constant < right.constant) {
			// left constant is more selective, prune right
			return ValueComparisonResult::PRUNE_RIGHT;
		} else if (left.constant > right.constant) {
			// right constant is more selective, prune left
			return ValueComparisonResult::PRUNE_LEFT;
		} else {
			// constants are equivalent
			// however we can still have the scenario where one is [<=] and the other is [<]
			// we want to prune the [<=] because [<] is more selective
			// if left is [<=] we prune the left, else we prune the right
			if (left.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
				return ValueComparisonResult::PRUNE_LEFT;
			} else {
				return ValueComparisonResult::PRUNE_RIGHT;
			}
		}
	} else if (IsLessThan(left.comparison_type)) {
		assert(IsGreaterThan(right.comparison_type));
		// left is [<] and right is [>], in this case we can either
		// (1) prune nothing or
		// (2) return UNSATISFIABLE
		// the SMALLER THAN constant has to be greater than the BIGGER THAN constant
		if (left.constant >= right.constant) {
			return ValueComparisonResult::PRUNE_NOTHING;
		} else {
			return ValueComparisonResult::UNSATISFIABLE_CONDITION;
		}
	} else {
		// left is [>] and right is [<]
		assert(IsLessThan(right.comparison_type) && IsGreaterThan(left.comparison_type));
		return InvertValueComparisonResult(CompareValueInformation(right, left));
	}
}

unique_ptr<LogicalOperator> ObsoleteFilterRewriter::Rewrite(unique_ptr<LogicalOperator> node) {
	if (node->type == LogicalOperatorType::FILTER) {
		auto &filter = (LogicalFilter &)*node;
		unordered_map<Expression *, vector<ExpressionValueInformation>, ExpressionHashFunction, ExpressionEquality>
		    equivalence_sets;
		// filter node, here we perform the actual obsolete filter removal
		// first we remove any obsolete filter expressions
		for (size_t i = 0; i < node->expressions.size(); i++) {
			if (node->expressions[i]->GetExpressionClass() != ExpressionClass::COMPARISON) {
				continue;
			}
			auto &comparison = (ComparisonExpression &)*node->expressions[i];
			// we only consider numeric checks
			if (!TypeIsNumeric(comparison.left->return_type)) {
				continue;
			}
			if (comparison.type != ExpressionType::COMPARE_LESSTHAN &&
			    comparison.type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
			    comparison.type != ExpressionType::COMPARE_GREATERTHAN &&
			    comparison.type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
			    comparison.type != ExpressionType::COMPARE_EQUAL) {
				// only support [>, >=, <, <=, ==] expressions
				continue;
			}
			// check if one of the sides of the comparison is a constant value
			if (!(comparison.left->type == ExpressionType::VALUE_CONSTANT ||
			      comparison.right->type == ExpressionType::VALUE_CONSTANT)) {
				// one of the sides must be a constant comparison
				continue;
			}

			bool left_is_constant = comparison.left->type == ExpressionType::VALUE_CONSTANT ? true : false;
			auto non_constant_expression = left_is_constant ? comparison.right.get() : comparison.left.get();
			auto constant_expression =
			    (ConstantExpression *)(left_is_constant ? comparison.left.get() : comparison.right.get());
			// add information about this comparison to the equivalence set
			ExpressionValueInformation info;
			info.expression_index = i;
			info.constant = constant_expression->value;
			info.comparison_type =
			    left_is_constant ? ComparisonExpression::FlipComparisionExpression(comparison.type) : comparison.type;
			equivalence_sets[non_constant_expression].push_back(info);
		}
		bool prune_filter = false;
		vector<size_t> prune_set;
		// now iterate over the expression equality information
		for (auto &entry : equivalence_sets) {
			auto &info = entry.second;
			// we can only potentially simplify if there is more than one expression
			if (info.size() > 1) {
				// now for each expression check if we can use it to remove other
				for (size_t inner_idx = 0; inner_idx < info.size(); inner_idx++) {
					auto &inner = info[inner_idx];
					if (inner.expression_index == (size_t)-1) {
						// this node is already removed
						continue;
					}
					for (size_t outer_idx = 0; outer_idx < info.size(); outer_idx++) {
						auto &outer = info[outer_idx];
						if (inner_idx == outer_idx) {
							continue;
						}
						if (outer.expression_index == (size_t)-1) {
							// this node is already removed
							continue;
						}
						// both nodes are still not removed
						// check if we can either
						// (1) prune the inner node node or
						// (2) prune the outer node or
						// (3) prune the entire filter (in case of unsatisfiable condition)
						auto result = CompareValueInformation(inner, outer);
						if (result == ValueComparisonResult::PRUNE_LEFT) {
							prune_set.push_back(inner.expression_index);
							inner.expression_index = (size_t)-1;
							break;
						} else if (result == ValueComparisonResult::PRUNE_RIGHT) {
							prune_set.push_back(outer.expression_index);
							outer.expression_index = (size_t)-1;
							continue;
						} else if (result == ValueComparisonResult::UNSATISFIABLE_CONDITION) {
							prune_filter = true;
							break;
						}
					}
					if (prune_filter) {
						break;
					}
				}
			}
		}
		// check if any of the filter expressions are a constant FALSE or a constant TRUE
		// if there are then we can prune the entire filter
		for (size_t expr_idx = 0; expr_idx < node->expressions.size(); expr_idx++) {
			auto &child = node->expressions[expr_idx];
			if (child->type == ExpressionType::VALUE_CONSTANT) {
				auto &constant = (ConstantExpression &)*child;
				auto constant_value = constant.value.CastAs(TypeId::BOOLEAN);
				if (constant_value.is_null || !constant_value.value_.boolean) {
					// FALSE or NULL, we can prune entire filter
					prune_filter = true;
					break;
				} else {
					// TRUE, we can prune this node from the filter
					prune_set.push_back(expr_idx);
				}
			}
		}
		if (prune_filter) {
			// the filter is guaranteed to produce an empty result
			// set this flag in the filter
			filter.empty_result = true;
		} else if (prune_set.size() > 0) {
			// prune any removed expressions
			for (auto index : prune_set) {
				node->expressions[index] = nullptr;
			}
			for (size_t i = node->expressions.size(); i > 0; i--) {
				size_t expr_idx = i - 1;
				if (!node->expressions[expr_idx]) {
					node->expressions.erase(node->expressions.begin() + expr_idx);
				}
			}
		}
	}
	RewriteSubqueries subquery_rewriter;
	for (auto &it : node->expressions) {
		it->Accept(&subquery_rewriter);
	}
	for (size_t i = 0; i < node->children.size(); i++) {
		node->children[i] = Rewrite(move(node->children[i]));
	}
	return node;
}
