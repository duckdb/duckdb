//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/const_prefix_handle.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! ConstPrefixHandle provides static methods for read-only prefix operations.
class ConstPrefixHandle {
public:
	static constexpr NType PREFIX = NType::PREFIX;

	//! Get a const reference to the child slot of the prefix.
	static const NodePtr &ChildRef(const ART &art, ConstNodeHandle &handle) {
		return *reinterpret_cast<const NodePtr *>(handle.GetPtr() + art.PrefixCount() + 1);
	}

	//! Traverses and verifies the node and its subtree.
	static void Verify(ART &art, const NodePtr &node);

	//! Returns the string representation of the node using ToStringOptions.
	static string ToString(ART &art, const NodePtr &node, const ToStringOptions &options);

private:
	template <class F>
	static NodePtr Iterator(ART &art, NodePtr node, const bool exit_gate, F &&lambda) {
		while (node.HasMetadata() && node.GetType() == PREFIX) {
			ConstNodeHandle handle(art, node);
			auto data = handle.GetPtr();
			auto child = ChildRef(art, handle);

			lambda(handle, data, child);

			node = child;
			if (exit_gate && node.GetGateStatus() == GateStatus::GATE_SET) {
				break;
			}
		}
		return node;
	}
};

} // namespace duckdb
