//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix_handle.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! PrefixHandle is a mutable wrapper to access and modify a prefix node.
//! The prefix contains up to the ART's prefix size bytes, an additional byte for the count,
//! and a Node pointer to a child node.
//! PrefixHandle uses SegmentHandle for memory management and marks memory as modified.
//! For read-only access, use ConstPrefixHandle instead.
class PrefixHandle {
public:
	static constexpr NType PREFIX = NType::PREFIX;

	static constexpr uint8_t DEPRECATED_COUNT = 15;

public:
	PrefixHandle() = delete;
	PrefixHandle(const ART &art, const Node node);
	PrefixHandle(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node node, const uint8_t count);
	PrefixHandle(const PrefixHandle &) = delete;
	PrefixHandle &operator=(const PrefixHandle &) = delete;
	PrefixHandle(PrefixHandle &&other) noexcept;
	PrefixHandle &operator=(PrefixHandle &&other) noexcept;

	data_ptr_t data;
	Node *ptr;

public:
	static inline uint8_t Count(const ART &art) {
		return art.prefix_count;
	}

	static void TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator);

private:
	PrefixHandle TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator,
	                                         const uint8_t byte);

private:
	unique_ptr<SegmentHandle> segment_handle;
};

} // namespace duckdb
