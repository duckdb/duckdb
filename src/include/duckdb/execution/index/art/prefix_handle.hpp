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

//! PrefixHandle provides static methods for mutable prefix operations.
class PrefixHandle {
public:
	static constexpr NType PREFIX = NType::PREFIX;
	static constexpr uint8_t DEPRECATED_COUNT = 15;

public:
	//! Create a new deprecated prefix node and return a handle to it.
	static NodeHandle NewDeprecated(FixedSizeAllocator &allocator, Node &node);

	//! Transform prefix chain to deprecated format.
	//! Returns an empty Node if the prefix was not loaded from storage (early out) or if the endpoint
	//! was a gated node (handled internally). Otherwise, returns a copy of the child pointer at the tail of
	//! the prefix chain for further traversal.
	static Node TransformToDeprecated(ART &art, Node &node, TransformToDeprecatedState &state);

private:
	static NodeHandle TransformToDeprecatedAppend(NodeHandle handle, ART &art, FixedSizeAllocator &allocator,
	                                              const uint8_t byte);
};

} // namespace duckdb
