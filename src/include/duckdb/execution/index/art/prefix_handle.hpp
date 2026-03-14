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
	//! nullptr denotes an early out optimization (the prefix has not been loaded from storage, hence we do not need
	//! to transform it. Otherwise, we get a pointer to the child node at the end of the prefix chain.
	static optional_ptr<NodePointer> TransformToDeprecated(ART &art, NodePointer &node,
	                                                       TransformToDeprecatedState &state);

private:
	static NodeHandle TransformToDeprecatedAppend(NodeHandle handle, ART &art, FixedSizeAllocator &allocator,
	                                              const uint8_t byte);
};

} // namespace duckdb
