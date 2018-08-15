#include "optimizer/rewriter.hpp"
#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
template<class RULETYPE, class NODETYPE, class TREETYPE>
unique_ptr<TREETYPE>
Rewriter<RULETYPE, NODETYPE, TREETYPE>::ApplyRules(unique_ptr<TREETYPE> root) {
	bool fixed_point;

	do {
		fixed_point = true;
		for (auto iterator = root->begin(); iterator != root->end();
		     iterator++) {
			auto &vertex = *iterator;
			for (auto &rule : rules) {
				vector<TREETYPE *> bindings;
				bool match = MatchOperands(rule->root.get(), vertex, bindings);
				if (!match) {
					continue;
				}

				auto new_vertex = rule->Apply(vertex, bindings);
				if (!new_vertex) { // rule returns input vertex if it does not
					               // apply
					continue;
				}
				fixed_point = false;

				if (&vertex == root.get()) {
					root = move(new_vertex);
					return ApplyRules(move(root));
				} else {
					iterator.replace(move(new_vertex));
				}
				break;
			}
		}
	} while (!fixed_point);
	return move(root);
}

template<class RULETYPE, class NODETYPE, class TREETYPE>
bool Rewriter<RULETYPE, NODETYPE, TREETYPE>::MatchOperands(NODETYPE *node,
                                       TREETYPE &rel,
                                       vector<TREETYPE *> &bindings) {

	if (!node->Matches(rel)) {
		return false;
	}

	bindings.push_back(&rel);
	switch (node->child_policy) {
	case ChildPolicy::ANY:
		return true;
	case ChildPolicy::UNORDERED: {
		if (rel.children.size() < node->children.size()) {
			return false;
		}
		// For each operand, at least one child must match. If
		// matchAnyChildren, usually there's just one operand.
		for (auto &c : rel.children) {
			bool match = false;
			for (auto &co : node->children) {
				match = MatchOperands(co.get(), *c, bindings);
				if (match) {
					break;
				}
			}
			if (!match) {
				return false;
			}
		}
		return true;
	}
	default: { // proceed along child ops and compare
		int n = node->children.size();
		if (rel.children.size() < n) {
			return false;
		}
		for (size_t i = 0; i < n; i++) {
			bool match = MatchOperands(node->children[i].get(),
			                           *rel.children[i], bindings);
			if (!match) {
				return false;
			}
		}
		return true;
	}
	}
}

template class Rewriter<ExpressionRule, ExpressionNode, AbstractExpression>;
template class Rewriter<LogicalRule, LogicalNode, LogicalOperator>;
}
