//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/rule.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/exception.hpp"
#include "parser/expression/list.hpp"
#include "planner/logical_operator.hpp"

#include <algorithm>
#include <string>
#include <vector>

namespace duckdb {

enum class AbstractOperatorType { LOGICAL_OPERATOR = 0, ABSTRACT_EXPRESSION = 1 };

class AbstractOperatorIterator;
class Rewriter;

class AbstractOperator : public Printable {
public:
	AbstractOperatorType type;
	union {
		LogicalOperator *op;
		Expression *expr;
	} value;
	AbstractOperator(LogicalOperator *op) : type(AbstractOperatorType::LOGICAL_OPERATOR) {
		value.op = op;
	}

	AbstractOperator(Expression *expr) : type(AbstractOperatorType::ABSTRACT_EXPRESSION) {
		value.expr = expr;
	}

	vector<AbstractOperator> GetAllChildren() {
		vector<AbstractOperator> result;
		if (type == AbstractOperatorType::LOGICAL_OPERATOR) {
			for (size_t i = 0; i < value.op->ExpressionCount(); i++) {
				result.push_back(AbstractOperator(value.op->GetExpression(i)));
			}
			for (auto &op : value.op->children) {
				result.push_back(AbstractOperator(op.get()));
			}
		} else { // AbstractOperatorType::ABSTRACT_EXPRESSION
			value.expr->EnumerateChildren([&](Expression* expr) {
				result.push_back(AbstractOperator(expr));
			});
		}
		return result;
	}

	typedef AbstractOperatorIterator iterator;

	iterator begin();

	iterator end();

	string ToString() const override {
		if (type == AbstractOperatorType::LOGICAL_OPERATOR) {
			return value.op->ToString();
		} else {
			return value.expr->ToString();
		}
	}
};

class AbstractOperatorIterator {
public:
	typedef AbstractOperatorIterator self_type;
	typedef AbstractOperator value_type;
	typedef AbstractOperator &reference;
	typedef AbstractOperator *pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef int difference_type;
	AbstractOperatorIterator(LogicalOperator *root, size_t op_index = 0, size_t expr_index = 0) {
		nodes.push(Node(root, op_index, expr_index));
	}

	void Next() {
		auto &child = nodes.top();
		if (child.node.type == AbstractOperatorType::LOGICAL_OPERATOR) {
			LogicalOperator *op = child.node.value.op;
			if (child.expr_index < op->ExpressionCount()) {
				nodes.push(Node(op->GetExpression(child.expr_index), 0));
				child.expr_index++;
			} else if (child.op_index < op->children.size()) {
				nodes.push(Node(op->children[child.op_index].get(), 0, 0));
				child.op_index++;
			} else {
				if (nodes.size() > 1) {
					nodes.pop();
					Next();
				}
			}

		} else { // AbstractOperatorType::ABSTRACT_EXPRESSION
			Expression *expr = child.node.value.expr;

			if (child.expr_index < expr->children.size()) {
				nodes.push(Node(expr->children[child.expr_index].get(), 0));
				child.expr_index++;
			} else if (expr->type == ExpressionType::SELECT_SUBQUERY && child.op_index == 0) {
				auto subquery = (SubqueryExpression *)expr;
				nodes.push(Node(subquery->op.get(), 0, 0));
				child.op_index++;
			} else {
				assert(nodes.size() > 1);
				nodes.pop();
				Next();
			}
		}
	}
	self_type operator++() {
		Next();
		return *this;
	}
	self_type operator++(int junk) {
		Next();
		return *this;
	}
	reference operator*() {
		return nodes.top().node;
	}
	pointer operator->() {
		return &nodes.top().node;
	}
	bool operator==(const self_type &rhs) {
		return nodes.size() == rhs.nodes.size() && nodes.top().op_index == rhs.nodes.top().op_index &&
		       nodes.top().expr_index == rhs.nodes.top().expr_index;
	}
	bool operator!=(const self_type &rhs) {
		return !(*this == rhs);
	}

	void replace(unique_ptr<LogicalOperator> new_op) {
		assert(nodes.top().node.type == AbstractOperatorType::LOGICAL_OPERATOR);
		nodes.pop();
		auto &parent = nodes.top();
		parent.op_index--;
		if (parent.node.type == AbstractOperatorType::LOGICAL_OPERATOR) {
			parent.node.value.op->children[parent.op_index] = std::move(new_op);
		} else {
			// SubqueryExpression
			auto expr = parent.node.value.expr;
			assert(expr->type == ExpressionType::SELECT_SUBQUERY);
			((SubqueryExpression *)expr)->op = std::move(new_op);
		}
	}

	void replace(unique_ptr<Expression> new_exp) {
		assert(nodes.top().node.type == AbstractOperatorType::ABSTRACT_EXPRESSION);
		nodes.pop();
		auto &parent = nodes.top();
		parent.expr_index--;

		if (parent.node.type == AbstractOperatorType::LOGICAL_OPERATOR) {
			parent.node.value.op->SetExpression(parent.expr_index, std::move(new_exp));
		} else { // AbstractOperatorType::ABSTRACT_EXPRESSION
			parent.node.value.expr->children[parent.expr_index] = std::move(new_exp);
		}
	}

private:
	struct Node {
		AbstractOperator node;
		size_t op_index;
		size_t expr_index;

		Node(LogicalOperator *op, size_t op_index, size_t expr_index)
		    : node(AbstractOperator(op)), op_index(op_index), expr_index(expr_index) {
		}

		Node(Expression *expr, size_t expr_index) : node(AbstractOperator(expr)), op_index(0), expr_index(expr_index) {
		}
	};
	std::stack<Node> nodes;
};

class AbstractRuleNode {
public:
	vector<unique_ptr<AbstractRuleNode>> children;
	ChildPolicy child_policy;

	AbstractRuleNode() : child_policy(ChildPolicy::ANY) {
	}
	virtual bool Matches(AbstractOperator &rel) = 0;
	virtual ~AbstractRuleNode() {
	}
};

class ExpressionNodeSet : public AbstractRuleNode {
public:
	vector<ExpressionType> types;
	ExpressionNodeSet(vector<ExpressionType> types) : types(types) {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::ABSTRACT_EXPRESSION &&
		       std::find(types.begin(), types.end(), rel.value.expr->type) != types.end();
	}
};

class ExpressionNodeType : public AbstractRuleNode {
public:
	ExpressionType type;
	ExpressionNodeType(ExpressionType type) : type(type) {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::ABSTRACT_EXPRESSION && rel.value.expr->type == type;
	}
};

class ComparisonNodeType : public AbstractRuleNode {
public:
	ComparisonNodeType() {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::ABSTRACT_EXPRESSION &&
		       rel.value.expr->type >= ExpressionType::COMPARE_BOUNDARY_START &&
		       rel.value.expr->type <= ExpressionType::COMPARE_BOUNDARY_END;
	}
};

class ColumnRefNodeDepth : public ExpressionNodeType {
public:
	size_t depth;
	ColumnRefNodeDepth(size_t depth) : ExpressionNodeType(ExpressionType::COLUMN_REF), depth(depth) {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return ExpressionNodeType::Matches(rel) && ((ColumnRefExpression *)rel.value.expr)->depth == depth;
	}
};

class ExpressionNodeAny : public AbstractRuleNode {
public:
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::ABSTRACT_EXPRESSION;
	}
};

class LogicalNodeSet : public AbstractRuleNode {
public:
	vector<LogicalOperatorType> types;
	LogicalNodeSet(vector<LogicalOperatorType> types) : types(types) {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::LOGICAL_OPERATOR &&
		       std::find(types.begin(), types.end(), rel.value.op->type) != types.end();
	}
};

class LogicalNodeType : public AbstractRuleNode {
public:
	LogicalOperatorType type;
	LogicalNodeType(LogicalOperatorType type) : type(type) {
	}
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::LOGICAL_OPERATOR && rel.value.op->type == type;
	}
};

class LogicalNodeAny : public AbstractRuleNode {
public:
	virtual bool Matches(AbstractOperator &rel) {
		return rel.type == AbstractOperatorType::LOGICAL_OPERATOR;
	}
};

class Rule {
public:
	unique_ptr<AbstractRuleNode> root;
	virtual unique_ptr<Expression> Apply(Rewriter &rewriter, Expression &root, vector<AbstractOperator> &bindings,
	                                     bool &fixed_point) {
		throw NotImplementedException("Apply Expression");
	};
	virtual unique_ptr<LogicalOperator> Apply(Rewriter &rewriter, LogicalOperator &root,
	                                          vector<AbstractOperator> &bindings, bool &fixed_point) {
		throw NotImplementedException("Apply LogicalOperator");
	};
	virtual ~Rule() {
	}
};

} // namespace duckdb
