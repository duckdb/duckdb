#include "optimizer/rewriter.hpp"
#include "optimizer/rule.hpp"

#include "common/exception.hpp"

using namespace duckdb;
using namespace std;

namespace duckdb {
unique_ptr<LogicalOperator>
Rewriter::ApplyRules(unique_ptr<LogicalOperator> root) {
	bool finished_iterating;

	do {
		finished_iterating = true;
		AbstractOperator op(root.get());
		for (auto iterator = op.begin(); iterator != op.end(); iterator++) {
			auto &vertex = *iterator;

			for (auto &rule : rules) {
				vector<AbstractOperator> bindings;
				bool match = MatchOperands(rule->root.get(), vertex, bindings);
				if (!match) {
					continue;
				}

				if (vertex.type == AbstractOperatorType::LOGICAL_OPERATOR) {
					auto new_vertex = rule->Apply(*this, *vertex.value.op,
					                              bindings, finished_iterating);
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
					auto new_vertex = rule->Apply(*this, *vertex.value.expr,
					                              bindings, finished_iterating);
					if (!new_vertex) {
						continue;
					}

					// abstract expressions cannot be the root of the plan
					iterator.replace(move(new_vertex));
				}
				finished_iterating = false;
				break;
			}
		}
	} while (!finished_iterating);
	return root;
}

bool Rewriter::MatchOperands(AbstractRuleNode *node, AbstractOperator rel,
                             vector<AbstractOperator> &bindings) {

	if (!node->Matches(rel)) {
		return false;
	}
	auto children = rel.GetAllChildren();

	vector<AbstractOperator> current_bindings = {rel};

	switch (node->child_policy) {
	case ChildPolicy::ALWAYS_MATCH:
		// ChildPolicy::ALWAYS_MATCH simply always matches
		break;
	case ChildPolicy::ANY: {
		// with ChildPolicy::Any only a single child must match
		bool match = false;
		for (auto &c : children) {
			for (auto &co : node->children) {
				match = MatchOperands(co.get(), c, current_bindings);
				if (match) {
					break;
				}
			}
			if (match) {
				break;
			}
		}
		if (match) {
			return true;
		}
		break;
	}
	case ChildPolicy::UNORDERED: {
		// ChildPolicy::UNORDERED requires all children to match to exactly one node
		// and none of the child nodes should be left unmatched

		// FIXME: unordered should match all of node->children of the node!
		// current implementation is not correct
		if (children.size() != node->children.size()) {
			return false;
		}
		// For each operand, at least one child must match. If
		// matchAnyChildren, usually there's just one operand.
		for (auto &c : children) {
			bool match = false;
			for (auto &co : node->children) {
				match = MatchOperands(co.get(), c, current_bindings);
				if (match) {
					break;
				}
			}
			if (!match) {
				return false;
			}
		}
		break;
	}
	case ChildPolicy::ORDERED: {
		// ChildPolicy::ORDERED requires the children to match exactly in order
		auto n = node->children.size();
		if (children.size() < n) {
			return false;
		}
		for (size_t i = 0; i < n; i++) {
			bool match = MatchOperands(node->children[i].get(), children[i],
			                           current_bindings);
			if (!match) {
				return false;
			}
		}
		break;
	}
	case ChildPolicy::SOME: {
		// ChildPolicy::SOME requires all nodes to find a match in node->children
		// for one child, ChildPolicy::SOME and ChildPolicy::ANY is identical
		if (children.size() < node->children.size()) {
			return false;
		}
		for (auto &co : node->children) {
			bool match = false;
			for (auto &c : children) {
				match = MatchOperands(co.get(), c, current_bindings);
				if (match) {
					break;
				}
			}
			if (!match) {
				return false;
			}
		}
		break;
	}

	default:
		throw NotImplementedException("Unsupported Child Policy");
	}

	bindings.insert(bindings.end(), current_bindings.begin(),
	                current_bindings.end());
	return true;
}

} // namespace duckdb
