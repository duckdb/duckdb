//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_scanner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

enum class ARTScanHandling : uint8_t {
	EMPLACE,
	POP,
};

//! ARTScanner scans the entire ART and processes each node.
template <ARTScanHandling HANDLING, class NODE>
class ARTScanner {
public:
	template <class FUNC>
	explicit ARTScanner(ART &art, FUNC &&handler, NODE &root) : art(art) {
		Emplace(handler, root);
	}

public:
	template <class FUNC>
	void Scan(FUNC &&handler) {
		while (!s.empty()) {
			auto &entry = s.top();
			if (entry.exhausted) {
				Pop(handler, entry.node);
				continue;
			}
			entry.exhausted = true;

			const auto type = entry.node.GetType();
			switch (type) {
			case NType::LEAF_INLINED:
			case NType::LEAF:
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF:
				break;
			case NType::PREFIX: {
				Prefix prefix(art, entry.node, true);
				Emplace(handler, *prefix.ptr);
				break;
			}

			case NType::NODE_4: {
				IterateChildren<FUNC, Node4>(handler, entry.node, type);
				break;
			}
			case NType::NODE_16: {
				IterateChildren<FUNC, Node16>(handler, entry.node, type);
				break;
			}
			case NType::NODE_48: {
				IterateChildren<FUNC, Node48>(handler, entry.node, type);
				break;
			}
			case NType::NODE_256: {
				IterateChildren<FUNC, Node256>(handler, entry.node, type);
				break;
			}
			default:
				throw InternalException("invalid node type for ART ARTScanner: %d", type);
			}
		}
	}

private:
	template <class FUNC>
	void Emplace(FUNC &&handler, NODE &node) {
		if (HANDLING == ARTScanHandling::EMPLACE) {
			auto result = handler(node);
			if (result == ARTHandlingResult::SKIP) {
				return;
			}
			D_ASSERT(result == ARTHandlingResult::CONTINUE);
		}
		s.emplace(node);
	}

	template <class FUNC>
	void Pop(FUNC &&handler, NODE &node) {
		if (HANDLING == ARTScanHandling::POP) {
			handler(node);
		}
		s.pop();
	}

	template <class FUNC, class NODE_TYPE>
	void IterateChildren(FUNC &&handler, NODE &node, const NType type) {
		auto &n = NodePointer::Ref<NODE_TYPE>(art, node, type);
		NODE_TYPE::Iterator(n, [&](NODE &child) { Emplace(handler, child); });
	}

private:
	struct NodeEntry {
		NodeEntry() = delete;
		explicit NodeEntry(NODE &node) : node(node), exhausted(false) {};

		NODE &node;
		bool exhausted;
	};

	ART &art;
	stack<NodeEntry> s;
};

//===--------------------------------------------------------------------===//
// Buffer-managed scan
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

template <class NODE_HANDLER, class CHILD_HANDLER>
void BufferManagedScan(ART &art, NodePointer &root, NODE_HANDLER &&node_handler, CHILD_HANDLER &&child_handler) {
	vector<NodePointer> stack;

	// root node is always pinned, handle it first.
	auto push_node = child_handler(root);
	if (push_node.HasMetadata()) {
		stack.push_back(push_node);
	}

	while (!stack.empty()) {
		NodePointer current = stack.back();
		stack.pop_back();

		// node_handler
		if (node_handler(current) == ScanNodeResult::SKIP) {
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
			throw InternalException("invalid node type for BufferManagedScan: %d", static_cast<int>(current.GetType()));
		}
	}
}

} // namespace duckdb
