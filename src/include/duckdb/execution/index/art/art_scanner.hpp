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

template <class NODE_TYPE, class CHILD_HANDLER>
static void ScanChildren(ART &art, NodePointer node, CHILD_HANDLER &&child_handler, vector<NodePointer> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE_TYPE>();
	NODE_TYPE::Iterator(n, [&](NodePointer &child) {
		auto push = child_handler(child);
		if (push.HasMetadata()) {
			stack.push_back(push);
		}
	});
}

template <class PRE_HANDLER, class CHILD_HANDLER>
void ARTScanPreOrder(ART &art, NodePointer &root, PRE_HANDLER &&pre_handler, CHILD_HANDLER &&child_handler) {
	vector<NodePointer> stack;

	// root node is always pinned, handle it first.
	auto push_node = child_handler(root);
	if (push_node.HasMetadata()) {
		stack.push_back(push_node);
	}

	while (!stack.empty()) {
		NodePointer current = stack.back();
		stack.pop_back();

		if (pre_handler(current) == ScanNodeResult::SKIP) {
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
			push_node = child_handler(child);
			if (push_node.HasMetadata()) {
				stack.push_back(push_node);
			}
			break;
		}
		case NType::NODE_4:
			ScanChildren<Node4>(art, current, child_handler, stack);
			break;
		case NType::NODE_16:
			ScanChildren<Node16>(art, current, child_handler, stack);
			break;
		case NType::NODE_48:
			ScanChildren<Node48>(art, current, child_handler, stack);
			break;
		case NType::NODE_256:
			ScanChildren<Node256>(art, current, child_handler, stack);
			break;
		default:
			throw InternalException("invalid node type for ARTScanner: %d", static_cast<int>(current.GetType()));
		}
	}
}

//===--------------------------------------------------------------------===//
// ARTScanPostOrder
//===--------------------------------------------------------------------===//

struct ScanEntry {
	ScanEntry(NodePointer node_p, bool children_scanned_p) : node(node_p), children_scanned(children_scanned_p) {
	}

	NodePointer node;
	bool children_scanned;
};

template <class NODE_TYPE, class CHILD_HANDLER>
static void ScanChildren(ART &art, NodePointer node, CHILD_HANDLER &&child_handler, vector<ScanEntry> &stack) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<NODE_TYPE>();
	NODE_TYPE::Iterator(n, [&](NodePointer &child) {
		auto push = child_handler(child);
		if (push.HasMetadata()) {
			stack.push_back(ScanEntry {push, false});
		}
	});
}

template <class CHILD_HANDLER, class POST_HANDLER>
void ARTScanPostOrder(ART &art, NodePointer &root, CHILD_HANDLER &&child_handler, POST_HANDLER &&post_handler) {
	vector<ScanEntry> stack;

	auto push_node = child_handler(root);
	if (push_node.HasMetadata()) {
		stack.push_back(ScanEntry {push_node, false});
	}

	while (!stack.empty()) {
		auto &entry = stack.back();

		if (entry.children_scanned) {
			post_handler(entry.node);
			stack.pop_back();
			continue;
		}

		entry.children_scanned = true;
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
			push_node = child_handler(child);
			if (push_node.HasMetadata()) {
				stack.push_back(ScanEntry {push_node, false});
			}
			break;
		}
		case NType::NODE_4:
			ScanChildren<Node4>(art, current, child_handler, stack);
			break;
		case NType::NODE_16:
			ScanChildren<Node16>(art, current, child_handler, stack);
			break;
		case NType::NODE_48:
			ScanChildren<Node48>(art, current, child_handler, stack);
			break;
		case NType::NODE_256:
			ScanChildren<Node256>(art, current, child_handler, stack);
			break;
		default:
			throw InternalException("invalid node type for ARTScanner: %d", static_cast<int>(current.GetType()));
		}
	}
}

} // namespace duckdb
