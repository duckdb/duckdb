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
	static const Node &ChildRef(const ART &art, ConstNodeHandle &handle) {
		return *reinterpret_cast<const Node *>(handle.GetPtr() + art.PrefixCount() + 1);
	}

	//! Traverses and verifies the node and its subtree.
	static void Verify(ART &art, const Node &node);

	//! Returns the string representation of the node using ToStringOptions.
	static string ToString(ART &art, const Node &node, const ToStringOptions &options);

private:
	template <class F>
	static void Iterator(ART &art, reference<const Node> &ref, const bool exit_gate, F &&lambda) {
		while (ref.get().HasMetadata() && ref.get().GetType() == PREFIX) {
			ConstNodeHandle handle(art, ref);
			auto data = handle.GetPtr();
			auto &child = ChildRef(art, handle);

			lambda(handle, data, child);

			ref = child;
			if (exit_gate && ref.get().GetGateStatus() == GateStatus::GATE_SET) {
				break;
			}
		}
	}
};

} // namespace duckdb
