//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/prefix_handle.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ARTScanPreOrder
//===--------------------------------------------------------------------===//

enum class ScanNodeResult : uint8_t { SCAN_CHILDREN, SKIP };

enum class ARTScanAction : uint8_t { PUSH_NODE, SKIP };

struct ARTScanStep {
	ARTScanStep(ARTScanAction action_p, Node node_p = Node()) : action(action_p), node(node_p) {
	}

	static ARTScanStep Push(Node node) {
		return ARTScanStep(ARTScanAction::PUSH_NODE, node);
	}

	static ARTScanStep Skip() {
		return ARTScanStep(ARTScanAction::SKIP);
	}

	ARTScanAction action;
	Node node;
};

//! Pins the parent node and calls preorder_handler on all its children. The handler can update each child pointer
//! in place and returns whether a child should be pushed onto the stack for further traversal.
template <class NODE, class PRE_HANDLER>
static void ScanChildren(ART &art, Node node, PRE_HANDLER &&pre_handler, vector<Node> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE>();
	NODE::Iterator(n, [&](Node &child) {
		auto step = pre_handler(child);
		if (step.action == ARTScanAction::PUSH_NODE) {
			stack.push_back(step.node);
		}
	});
}

//! Pre-order scanner: each child pointer in the parent node is processed by pre_handler before being pushed onto the
//! stack (either in its original or processed form). When a node is popped, scan_strategy decides whether to scan
//! its children or skip it. If the child pointers need to be scanned, the popped node is pinned in ScanChildren and
//! any updates are performed in place within that pinned node using preorder_handler (which also defines what Node to
//! push onto the stack for further traversal).
template <class SCAN_STRATEGY, class PRE_HANDLER>
void ARTScanPreorder(ART &art, Node &root, SCAN_STRATEGY &&scan_strategy, PRE_HANDLER &&preorder_handler) {
	vector<Node> stack;

	// The root node pointer lives in the ART object, not inside a fixed-size buffer.
	auto step = preorder_handler(root);
	if (step.action == ARTScanAction::PUSH_NODE) {
		stack.push_back(step.node);
	}

	while (!stack.empty()) {
		Node current = stack.back();
		stack.pop_back();

		if (scan_strategy(current) == ScanNodeResult::SKIP) {
			continue;
		}

		switch (current.GetType()) {
		case NType::LEAF_INLINED:
		case NType::LEAF:
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
			break;
		case NType::PREFIX: {
			NodeHandle handle(art, current);
			auto &child = PrefixHandle::ChildRef(art, handle);
			step = preorder_handler(child);
			if (step.action == ARTScanAction::PUSH_NODE) {
				stack.push_back(step.node);
			}
			break;
		}
		case NType::NODE_4:
			ScanChildren<Node4>(art, current, preorder_handler, stack);
			break;
		case NType::NODE_16:
			ScanChildren<Node16>(art, current, preorder_handler, stack);
			break;
		case NType::NODE_48:
			ScanChildren<Node48>(art, current, preorder_handler, stack);
			break;
		case NType::NODE_256:
			ScanChildren<Node256>(art, current, preorder_handler, stack);
			break;
		default:
			throw InternalException("invalid node type for ARTScanPreOrder: %d", current.GetType());
		}
	}
}

//===--------------------------------------------------------------------===//
// ARTScanPostOrder
//===--------------------------------------------------------------------===//

struct ScanEntry {
	ScanEntry(Node node_p, bool children_scanned_p) : node(node_p), children_visited(children_scanned_p) {
	}

	Node node;
	bool children_visited;
};

//! Pins the parent node and iterates over all its children. The child_selector receives each child by reference
//! and returns whether a child should be pushed onto the stack for further traversal.
template <class NODE, class CHILD_SELECTOR>
static void ScanChildren(ART &art, Node node, CHILD_SELECTOR &&child_selector, vector<ScanEntry> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE>();
	NODE::Iterator(n, [&](Node &child) {
		auto step = child_selector(child);
		if (step.action == ARTScanAction::PUSH_NODE) {
			stack.push_back(ScanEntry {step.node, false});
		}
	});
}

//! Post-order scanner: each node is visited twice via the children_visited flag in ScanEntry.
//! On the first visit (children_visited = false), the node is marked as visited and the child_selector decides which
//! children to push onto the stack.
//! On the second visit (children_visited = true, after all descendants have been processed),
//! post_handler fires on the node and then we pop it from the stack.
template <class CHILD_SELECTOR, class POST_HANDLER>
void ARTScanPostorder(ART &art, Node &root, CHILD_SELECTOR &&child_selector, POST_HANDLER &&postorder_handler) {
	vector<ScanEntry> stack;

	D_ASSERT(root.HasMetadata());
	stack.push_back(ScanEntry {root, false});

	while (!stack.empty()) {
		auto &entry = stack.back();

		if (entry.children_visited) {
			postorder_handler(entry.node);
			stack.pop_back();
			continue;
		}

		entry.children_visited = true;
		auto current = entry.node;

		switch (current.GetType()) {
		case NType::LEAF_INLINED:
		case NType::LEAF:
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
			break;
		case NType::PREFIX: {
			NodeHandle handle(art, current);
			auto &child = PrefixHandle::ChildRef(art, handle);
			auto step = child_selector(child);
			if (step.action == ARTScanAction::PUSH_NODE) {
				stack.push_back(ScanEntry {step.node, false});
			}
			break;
		}
		case NType::NODE_4:
			ScanChildren<Node4>(art, current, child_selector, stack);
			break;
		case NType::NODE_16:
			ScanChildren<Node16>(art, current, child_selector, stack);
			break;
		case NType::NODE_48:
			ScanChildren<Node48>(art, current, child_selector, stack);
			break;
		case NType::NODE_256:
			ScanChildren<Node256>(art, current, child_selector, stack);
			break;
		default:
			throw InternalException("invalid node type for ARTScanPostOrder: %d", current.GetType());
		}
	}
}

} // namespace duckdb
