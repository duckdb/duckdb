//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/scanner.hpp
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

enum class ARTScanHandlingMode : uint8_t {
	EMPLACE,
	POP,
};

enum class ARTScanResult : uint8_t {
	CONTINUE,
	SKIP,
	YIELD,
};

template <ARTScanHandlingMode MODE>
class ARTScanner {
public:
	explicit ARTScanner(ART &art) : art(art) {
	}

public:
	template <class FUNC>
	void Init(FUNC &&handler, Node &root) {
		Emplace(handler, root);
	}

	template <class FUNC>
	void Scan(FUNC &&handler) {
		while (!stack.empty()) {
			auto &entry = stack.top();
			if (entry.exhausted) {
				Pop(handler, entry.node);
				continue;
			}
			entry.exhausted = true;

			auto type = entry.node.GetType();
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
				throw InternalException("invalid node type for ART ScanHandleLast: %s.", EnumUtil::ToString(type));
			}
		}
	}

private:
	template <class FUNC>
	void Emplace(FUNC &&handler, Node &node) {
		if (MODE == ARTScanHandlingMode::EMPLACE) {
			auto result = handler(node);
			if (result == ARTScanResult::SKIP) {
				return;
			}
		}
		stack.emplace(node);
	}

	template <class FUNC>
	void Pop(FUNC &&handler, Node &node) {
		if (MODE == ARTScanHandlingMode::POP) {
			handler(node);
		}
		stack.pop();
	}

	template <class FUNC, class NODE>
	void IterateChildren(FUNC &&handler, Node &node, const NType type) {
		auto &n = Node::Ref<NODE>(art, node, type);
		NODE::Iterator(n, [&](Node &child) { Emplace(handler, child); });
	}

private:
	struct NodeEntry {
		explicit NodeEntry(Node &node) : node(node), exhausted(false) {};
		Node &node;
		bool exhausted;
	};

	ART &art;
	stack<NodeEntry> stack;
};

} // namespace duckdb
