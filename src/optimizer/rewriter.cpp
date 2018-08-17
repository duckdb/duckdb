#include "optimizer/rewriter.hpp"
#include "optimizer/rule.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
unique_ptr<LogicalOperator>
Rewriter::ApplyRules(unique_ptr<LogicalOperator> root) {
	bool fixed_point;

	do {
		fixed_point = true;
		AbstractOperator op(root.get());
		for (auto iterator = op.begin(); iterator != op.end(); iterator++) {
			auto &vertex = *iterator;
			for (auto &rule : rules) {
				vector<AbstractOperator *> bindings;
				bool match = MatchOperands(rule->root.get(), vertex, bindings);
				if (!match) {
					continue;
				}

				if (vertex.type == AbstractOperatorType::LOGICAL_OPERATOR) {
					auto new_vertex = rule->Apply(*vertex.value.op, bindings);
					if (!new_vertex) {
						continue;
					}

					if (vertex.value.op == root.get()) {
						// the node is the root of the plan, restart with the
						// new root
						return ApplyRules(move(new_vertex));
					} else {
						// node is not the root, replace it in the iterator
						iterator.replace(move(new_vertex));
					}

				} else { // AbstractOperatorType::ABSTRACT_EXPRESSION
					auto new_vertex = rule->Apply(*vertex.value.expr, bindings);
					if (!new_vertex) {
						continue;
					}

					// abstract expressions cannot be the root of the plan
					iterator.replace(move(new_vertex));
				}
				fixed_point = false;
				break;
			}
		}
	} while (!fixed_point);
	return move(root);
}

bool Rewriter::MatchOperands(AbstractRuleNode *node, AbstractOperator &rel,
                             vector<AbstractOperator *> &bindings) {

	if (!node->Matches(rel)) {
		return false;
	}
	auto children = rel.GetAllChildren();

	bindings.push_back(&rel);
	switch (node->child_policy) {
	case ChildPolicy::ANY:
		return true;
	case ChildPolicy::UNORDERED: {
		if (children.size() < node->children.size()) {
			return false;
		}
		// For each operand, at least one child must match. If
		// matchAnyChildren, usually there's just one operand.
		for (auto &c : children) {
			bool match = false;
			for (auto &co : node->children) {
				match = MatchOperands(co.get(), c, bindings);
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
		if (children.size() < n) {
			return false;
		}
		for (size_t i = 0; i < n; i++) {
			bool match =
			    MatchOperands(node->children[i].get(), children[i], bindings);
			if (!match) {
				return false;
			}
		}
		return true;
	}
	}
}

} // namespace duckdb
