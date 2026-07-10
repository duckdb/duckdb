//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/const_prefix_handle.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

//! The result of an on_pop handler: scan the children of the popped node, or skip them.
enum class ARTScanNodeResult : uint8_t { SCAN_CHILDREN, SKIP };

//! Pins node and passes each of its child slots to child_handler.
//! child_handler runs under the pin, can update the slot in place,
//! and returns the node to push, or an empty OptionalNode.
template <class NODE, class NODE_HANDLE, class ART_TYPE, class CHILD_HANDLER, class PUSH>
void ARTScanChildrenInternal(ART_TYPE &art, const Node node, CHILD_HANDLER &&child_handler, PUSH &&push) {
	NODE_HANDLE handle(art, node);
	auto &n = handle.template Get<NODE>();
	NODE::Iterator(n, [&](auto &child) {
		auto next = child_handler(child);
		if (next) {
			push(next.Get());
		}
	});
}

//! Dispatches on the node type and scans the children of current (see ARTScanChildrenInternal).
template <class NODE_HANDLE, class PREFIX, class ART_TYPE, class CHILD_HANDLER, class PUSH>
void ARTScanChildren(ART_TYPE &art, const Node current, CHILD_HANDLER &&child_handler, PUSH &&push) {
	switch (current.GetType()) {
	case NType::LEAF_INLINED:
	case NType::LEAF:
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		break;
	case NType::PREFIX: {
		NODE_HANDLE handle(art, current);
		auto &child = PREFIX::ChildRef(art, handle);
		auto next = child_handler(child);
		if (next) {
			push(next.Get());
		}
		break;
	}
	case NType::NODE_4:
		ARTScanChildrenInternal<Node4, NODE_HANDLE>(art, current, child_handler, push);
		break;
	case NType::NODE_16:
		ARTScanChildrenInternal<Node16, NODE_HANDLE>(art, current, child_handler, push);
		break;
	case NType::NODE_48:
		ARTScanChildrenInternal<Node48, NODE_HANDLE>(art, current, child_handler, push);
		break;
	case NType::NODE_256:
		ARTScanChildrenInternal<Node256, NODE_HANDLE>(art, current, child_handler, push);
		break;
	default:
		throw InternalException("invalid node type for ARTScanChildren: %d", current.GetType());
	}
}

//===--------------------------------------------------------------------===//
// ARTScanPreorder
//===--------------------------------------------------------------------===//

template <class NODE_HANDLE, class PREFIX, class ART_TYPE, class NODE_REF, class CHILD_HANDLER, class ON_POP>
void ARTScanPreorderInternal(ART_TYPE &art, NODE_REF &root, CHILD_HANDLER &&child_handler, ON_POP &&on_pop) {
	vector<Node> stack;
	auto push = [&stack](const Node node) {
		stack.push_back(node);
	};

	// The root node pointer lives in the ART object, not inside a fixed-size buffer.
	auto next = child_handler(root);
	if (next) {
		push(next.Get());
	}

	while (!stack.empty()) {
		Node current = stack.back();
		stack.pop_back();

		if (on_pop(current) == ARTScanNodeResult::SKIP) {
			continue;
		}
		ARTScanChildren<NODE_HANDLE, PREFIX>(art, current, child_handler, push);
	}
}

//! Pre-order scanner.
//! child_handler runs on the root and on each child slot. For child slots, it runs while the parent
//! is pinned and can update the slot in place. It returns the node to continue the traversal with,
//! or an empty OptionalNode to stop.
//! on_pop runs on each popped node with no pins held, and decides whether to scan its children.
//! Work that must not run under a pin belongs in on_pop.
template <class CHILD_HANDLER, class ON_POP>
void ARTScanPreorder(ART &art, Node &root, CHILD_HANDLER &&child_handler, ON_POP &&on_pop) {
	ARTScanPreorderInternal<NodeHandle, PrefixHandle>(art, root, child_handler, on_pop);
}

//! Pre-order scanner over an immutable ART (see ARTScanPreorder).
template <class CHILD_HANDLER, class ON_POP>
void ARTConstScanPreorder(const ART &art, const Node &root, CHILD_HANDLER &&child_handler, ON_POP &&on_pop) {
	ARTScanPreorderInternal<ConstNodeHandle, ConstPrefixHandle>(art, root, child_handler, on_pop);
}

//===--------------------------------------------------------------------===//
// ARTScanPostorder
//===--------------------------------------------------------------------===//

struct ScanEntry {
	ScanEntry(Node node_p, bool children_visited_p) : node(node_p), children_visited(children_visited_p) {
	}

	Node node;
	bool children_visited;
};

//! Post-order scanner: each node is visited twice via the children_visited flag in ScanEntry.
//! On the first visit, child_handler runs on each child slot (under the parent's pin) and returns
//! the node to push, or an empty OptionalNode.
//! On the second visit, after all descendants have been processed, post_handler runs on the node
//! with no pins held.
template <class CHILD_HANDLER, class POST_HANDLER>
void ARTScanPostorder(ART &art, Node &root, CHILD_HANDLER &&child_handler, POST_HANDLER &&post_handler) {
	vector<ScanEntry> stack;
	auto push = [&stack](const Node node) {
		stack.push_back(ScanEntry {node, false});
	};

	D_ASSERT(root.HasMetadata());
	stack.push_back(ScanEntry {root, false});

	while (!stack.empty()) {
		if (stack.back().children_visited) {
			auto current = stack.back().node;
			post_handler(current);
			stack.pop_back();
			continue;
		}

		auto current = stack.back().node;
		stack.back().children_visited = true;
		ARTScanChildren<NodeHandle, PrefixHandle>(art, current, child_handler, push);
	}
}

} // namespace duckdb
