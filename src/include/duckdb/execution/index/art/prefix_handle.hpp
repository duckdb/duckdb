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
	static NodeHandle NewDeprecated(FixedSizeAllocator &allocator, NodePointer &node);

	//! Transform prefix chain to deprecated format.
	//! Returns an empty NodePointer if the prefix was not loaded from storage (early out) or if the endpoint
	//! was a gated node (handled internally). Otherwise, returns a copy of the endpoint for further traversal.
	static NodePointer TransformToDeprecated(ART &art, NodePointer &node, TransformToDeprecatedState &state);

private:
	static NodeHandle TransformToDeprecatedAppend(NodeHandle handle, ART &art, FixedSizeAllocator &allocator,
	                                              const uint8_t byte);
};

} // namespace duckdb
