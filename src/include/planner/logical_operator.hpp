//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/logical_operator.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "catalog/catalog.hpp"

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/statement/select_statement.hpp"

#include "planner/logical_operator_visitor.hpp"

namespace duckdb {
//! LogicalOperator is the base class of the logical operators present in the
//! logical query tree
class LogicalOperator : public Printable {
  public:
	LogicalOperator(LogicalOperatorType type) : type(type) {}

	LogicalOperatorType GetOperatorType() { return type; }

	virtual std::string ParamsToString() const { return std::string(); }

	virtual std::string ToString() const override {
		std::string result = LogicalOperatorToString(type);
		result += ParamsToString();
		if (children.size() > 0) {
			result += "(";
			for (size_t i = 0; i < children.size(); i++) {
				auto &child = children[i];
				result += child->ToString();
				if (i < children.size() - 1) {
					result += ", ";
				}
			}
			result += ")";
		}

		return result;
	}

	virtual void Accept(LogicalOperatorVisitor *) = 0;
	virtual void AcceptChildren(LogicalOperatorVisitor *v) {
		for (auto &child : children) {
			child->Accept(v);
		}
	}

	//! The type of the logical operator
	LogicalOperatorType type;
	//! The set of children of the operator
	std::vector<std::unique_ptr<LogicalOperator>> children;


	class iterator {
	  public:
		typedef iterator self_type;
		typedef LogicalOperator value_type;
		typedef LogicalOperator &reference;
		typedef LogicalOperator *pointer;
		typedef std::forward_iterator_tag iterator_category;
		typedef int difference_type;
		iterator(LogicalOperator &root, size_t index = 0) {
			nodes.push(Node(root, index));
		}

		void Next() {
			auto &child = nodes.top();
			if (child.index < child.node.children.size()) {
				nodes.push(Node(*child.node.children[child.index], 0));
				child.index++;
			} else {
				if (nodes.size() > 1) {
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
		reference operator*() { return nodes.top().node; }
		pointer operator->() { return &nodes.top().node; }
		bool operator==(const self_type &rhs) {
			return nodes.size() == rhs.nodes.size() &&
			       nodes.top().index == rhs.nodes.top().index;
		}
		bool operator!=(const self_type &rhs) { return !(*this == rhs); }
		void replace(std::unique_ptr<LogicalOperator> new_vertex) {
			nodes.pop();
			auto &parent = nodes.top();
			parent.index--;
			parent.node.children[parent.index] = std::move(new_vertex);
		}

	  private:
		struct Node {
			LogicalOperator &node;
			size_t index;
			Node(LogicalOperator &node, size_t index)
			    : node(node), index(index) {}
		};
		std::stack<Node> nodes;
	};

	iterator begin() { return iterator(*this); }

	iterator end() { return iterator(*this, children.size()); }
};
} // namespace duckdb
