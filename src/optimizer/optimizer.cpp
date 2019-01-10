#include "optimizer/optimizer.hpp"

#include "optimizer/join_order_optimizer.hpp"
#include "optimizer/obsolete_filter_rewriter.hpp"
#include "optimizer/rule/list.hpp"
#include "optimizer/subquery_rewriter.hpp"
#include "parser/expression/common_subexpression.hpp"
#include "planner/operator/list.hpp"

using namespace duckdb;
using namespace std;

Optimizer::Optimizer(ClientContext &client_context, BindContext &context) : context(context), rewriter(client_context) {
	rewriter.rules.push_back(make_unique<ConstantFoldingRule>(rewriter));
	rewriter.rules.push_back(make_unique<DistributivityRule>(rewriter));
	rewriter.rules.push_back(make_unique<ArithmeticSimplificationRule>(rewriter));

#ifdef DEBUG
	for (auto &rule : rewriter.rules) {
		// root not defined in rule
		assert(rule->root);
	}
#endif
}

class OptimizeSubqueries : public LogicalOperatorVisitor {
public:
	using LogicalOperatorVisitor::Visit;
	unique_ptr<Expression> Visit(SubqueryExpression &subquery) override {
		// we perform join reordering within the subquery expression
		JoinOrderOptimizer optimizer;
		subquery.op = optimizer.Optimize(move(subquery.op));
		return nullptr;
	}
};

class CommonSubExpressionOptimizer : public LogicalOperatorVisitor {
public:
	struct CSENode {
		size_t count;
		Expression *expr;

		CSENode(size_t count = 1, Expression *expr = nullptr) : count(count), expr(expr) {
		}
	};
	typedef unordered_map<Expression *, CSENode, ExpressionHashFunction, ExpressionEquality> expression_map_t;
	void CountExpressions(Expression *expr, expression_map_t &expression_count) {
		if (expr->HasChildren()) {
			// we only consider expressions with children for CSE elimination
			auto node = expression_count.find(expr);
			if (node == expression_count.end()) {
				// first time we encounter this expression, insert this node with [count = 1]
				expression_count[expr] = CSENode(1);
			} else {
				// we encountered this expression before, increment the occurrence count
				node->second.count++;
			}
			// recursively count the children
			expr->EnumerateChildren([&](Expression *child) { CountExpressions(child, expression_count); });
		}
	}

	unique_ptr<Expression> PerformCSEElimination(unique_ptr<Expression> expr, expression_map_t &expression_count) {
		if (expr->HasChildren()) {
			// check if this child is eligible for CSE elimination
			assert(expression_count.find(expr.get()) != expression_count.end());
			auto &node = expression_count[expr.get()];
			if (node.count > 1) {
				// this expression occurs more than once! replace it with a CSE
				// check if it has already been replaced with a CSE before
				unique_ptr<CommonSubExpression> cse;
				string alias = expr->alias.empty() ? expr->GetName() : expr->alias;
				if (node.expr) {
					// it has! replace it with a CSE that just refers to this child
					cse = make_unique<CommonSubExpression>(node.expr);
				} else {
					// it has not! create the CSE with the ownership of this node
					node.expr = expr.get();
					cse = make_unique<CommonSubExpression>(move(expr));
				}
				cse->alias = alias;
				return cse;
			}
			// this expression only occurs once, we can't perform CSE elimination
			// look into the children to see if it is possible
			expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
				return PerformCSEElimination(move(child), expression_count);
			});
		}
		return expr;
	}

	//! Main method to extract common subexpressions
	void ExtractCommonSubExpresions(LogicalOperator &op) {
		// first we count for each expression with children how many types it occurs
		expression_map_t expression_count;
		for (auto &expr : op.expressions) {
			CountExpressions(expr.get(), expression_count);
		}
		// now we iterate over all the expressions and perform the actual CSE elimination
		for (size_t i = 0; i < op.expressions.size(); i++) {
			op.expressions[i] = PerformCSEElimination(move(op.expressions[i]), expression_count);
		}
	}

	using LogicalOperatorVisitor::Visit;
	void Visit(LogicalFilter &op) override {
		ExtractCommonSubExpresions(op);
	}
	void Visit(LogicalProjection &op) override {
		ExtractCommonSubExpresions(op);
	}
};

unique_ptr<LogicalOperator> Optimizer::Optimize(unique_ptr<LogicalOperator> plan) {
	// first we perform expression rewrites using the ExpressionRewriter
	// this does not change the logical plan structure, but only simplifies the expression trees
	rewriter.Apply(*plan);
	// now perform obsolete filter removal
	ObsoleteFilterRewriter obsolete_filter;
	plan = obsolete_filter.Rewrite(move(plan));
	// then we perform the join ordering optimization
	// this also rewrites cross products + filters into joins and performs filter pushdowns
	JoinOrderOptimizer optimizer;
	plan = optimizer.Optimize(move(plan));
	// perform join order optimization in subqueries as well
	OptimizeSubqueries opt;
	plan->Accept(&opt);
	// now we rewrite subqueries
	SubqueryRewriter subquery_rewriter(context);
	plan = subquery_rewriter.Rewrite(move(plan));
	// then we extract common subexpressions inside the different operators
	CommonSubExpressionOptimizer cse_optimizer;
	plan->Accept(&cse_optimizer);
	return plan;
}
