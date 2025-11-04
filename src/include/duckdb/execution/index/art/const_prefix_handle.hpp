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

//! ConstPrefixHandle is a read-only wrapper to access a prefix node.
//! A segment handle is used for memory management, but it is not marked as modified.
class ConstPrefixHandle {
public:
	static constexpr NType PREFIX = NType::PREFIX;

	ConstPrefixHandle() = delete;
	ConstPrefixHandle(const ART &art, const Node node);
	ConstPrefixHandle(const ConstPrefixHandle &) = delete;
	ConstPrefixHandle &operator=(const ConstPrefixHandle &) = delete;
	ConstPrefixHandle(ConstPrefixHandle &&) = delete;
	ConstPrefixHandle &operator=(ConstPrefixHandle &&) = delete;

public:
	uint8_t GetCount(const ART &art) const;
	uint8_t GetByte(const idx_t pos) const;

	static inline uint8_t Count(const ART &art) {
		return art.prefix_count;
	}

	//! Traverses and verifies the node and its subtree.
	static void Verify(ART &art, const Node &node);

	//! Returns the string representation of the node at indentation level.
	static string ToString(ART &art, const Node &node, idx_t indent_level, const bool inside_gate = false,
	                       const bool display_ascii = false);

private:
	template <class F>
	static void Iterator(ART &art, reference<const Node> &ref, const bool exit_gate, F &&lambda) {
		while (ref.get().HasMetadata() && ref.get().GetType() == PREFIX) {
			ConstPrefixHandle handle(art, ref);
			lambda(handle);

			ref = *handle.ptr;
			if (exit_gate && ref.get().GetGateStatus() == GateStatus::GATE_SET) {
				break;
			}
		}
	}

private:
	unique_ptr<SegmentHandle> segment_handle;
	data_ptr_t data;
	Node *ptr;
};

} // namespace duckdb
