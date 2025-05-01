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
				throw InternalException("invalid node type for ART ARTScanner: %s", EnumUtil::ToString(type));
			}
		}
	}

private:
	template <class FUNC>
	void Emplace(FUNC &&handler, NODE &node) {
		if (HANDLING == ARTScanHandling::EMPLACE) {
			if (handler(node) == ARTHandlingResult::SKIP) {
				return;
			}
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
		auto &n = Node::Ref<NODE_TYPE>(art, node, type);
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

} // namespace duckdb
