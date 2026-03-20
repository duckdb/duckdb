//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ARTScanPreOrder
//===--------------------------------------------------------------------===//

enum class ScanNodeResult : uint8_t { SCAN_CHILDREN, SKIP };

//! Pins the parent node and calls preorder_handler on all the children, which can perform in-place updates within
//! the parent while it is pinned, as well as defines the return NodePointer value that should be pushed onto the
//! stack for further traversal.
template <class NODE_TYPE, class PRE_HANDLER>
static void ScanChildren(ART &art, NodePointer node, PRE_HANDLER &&pre_handler, vector<NodePointer> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE_TYPE>();
	NODE_TYPE::Iterator(n, [&](NodePointer &child) {
		auto push = pre_handler(child);
		if (push.HasMetadata()) {
			stack.push_back(push);
		}
	});
}

//! Pre-order scanner: each child is processed by pre_handler before being pushed onto the stack.
//! When a node is popped, the filter decides whether to scan its children or skip it.
//! If the children need to be scanned, the parent is pinned in ScanChildren and any updates are performed in place
//! within the pinned parent node using preorder_handler (which also defines what NodePointer to push onto the stack
//! for further traversal).
template <class FILTER, class PRE_HANDLER>
void ARTScanPreorder(ART &art, NodePointer &root, FILTER &&filter, PRE_HANDLER &&preorder_handler) {
	vector<NodePointer> stack;

	// root node is always pinned, handle it first.
	auto push_node = preorder_handler(root);
	if (push_node.HasMetadata()) {
		stack.push_back(push_node);
	}

	while (!stack.empty()) {
		NodePointer current = stack.back();
		stack.pop_back();

		if (filter(current) == ScanNodeResult::SKIP) {
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
			auto &child = *reinterpret_cast<NodePointer *>(handle.GetPtr() + art.PrefixCount() + 1);
			push_node = preorder_handler(child);
			if (push_node.HasMetadata()) {
				stack.push_back(push_node);
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
	ScanEntry(NodePointer node_p, bool children_scanned_p) : node(node_p), children_visited(children_scanned_p) {
	}

	NodePointer node;
	bool children_visited;
};

//! Pins the parent node and iterates over all the children. The filter receives each child by reference
//! and returns the NodePointer value that should be pushed onto the stack for further traversal.
template <class NODE_TYPE, class FILTER>
static void ScanChildren(ART &art, NodePointer node, FILTER &&filter, vector<ScanEntry> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE_TYPE>();
	NODE_TYPE::Iterator(n, [&](NodePointer &child) {
		auto push_node = filter(child);
		if (push_node.HasMetadata()) {
			stack.push_back(ScanEntry {push_node, false});
		}
	});
}

//! Post-order scanner: each node is visited twice via the children_visited flag in ScanEntry.
//! On the first visit (children_visited = false), the node is marked as visited and the filter decides which
//! children to push onto the stack. The filter receives each child by reference and returns the NodePointer
//! to push for further traversal.
//! On the second visit (children_visited = true, after all descendants have been processed),
//! post_handler fires on the node and then we pop it from the stack.
template <class FILTER, class POST_HANDLER>
void ARTScanPostorder(ART &art, NodePointer &root, FILTER &&filter, POST_HANDLER &&postorder_handler) {
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
			auto &child = *reinterpret_cast<NodePointer *>(handle.GetPtr() + art.PrefixCount() + 1);
			auto push_node = filter(child);
			if (push_node.HasMetadata()) {
				stack.push_back(ScanEntry {push_node, false});
			}
			break;
		}
		case NType::NODE_4:
			ScanChildren<Node4>(art, current, filter, stack);
			break;
		case NType::NODE_16:
			ScanChildren<Node16>(art, current, filter, stack);
			break;
		case NType::NODE_48:
			ScanChildren<Node48>(art, current, filter, stack);
			break;
		case NType::NODE_256:
			ScanChildren<Node256>(art, current, filter, stack);
			break;
		default:
			throw InternalException("invalid node type for ARTScanPostOrder: %d", current.GetType());
		}
	}
}

} // namespace duckdb
