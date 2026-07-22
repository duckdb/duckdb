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
#include "duckdb/execution/index/art/node_handle.hpp"

namespace duckdb {

//! PrefixHandle owns the pin for a mutable prefix node.
class PrefixHandle {
public:
	static constexpr NType PREFIX = NType::PREFIX;
	static constexpr uint8_t DEPRECATED_COUNT = 15;

public:
	explicit PrefixHandle(NodeHandle &&handle_p) : handle(std::move(handle_p)) {
	}

	PrefixHandle(const PrefixHandle &) = delete;
	PrefixHandle &operator=(const PrefixHandle &) = delete;
	PrefixHandle(PrefixHandle &&) = default;
	PrefixHandle &operator=(PrefixHandle &&) = default;

public:
	data_ptr_t Data() {
		return handle.GetPtr();
	}

	uint8_t GetCount(const ART &art) {
		return Data()[art.PrefixCount()];
	}

	void SetCount(const ART &art, const uint8_t count) {
		Data()[art.PrefixCount()] = count;
	}

	uint8_t GetByte(const idx_t pos) {
		return Data()[pos];
	}

	void SetByte(const idx_t pos, const uint8_t byte) {
		Data()[pos] = byte;
	}

	//! Returns the child slot. The reference is valid while this PrefixHandle is alive.
	NodePtr &Child(const ART &art) {
		return ChildRef(art, handle);
	}

	NodeHandle TakeHandle() && {
		return std::move(handle);
	}

public:
	//! Create a new deprecated prefix node and return a handle to it.
	static NodeHandle NewDeprecated(FixedSizeAllocator &allocator, NodePtr &node);

	//! Get a mutable reference to the child slot of the prefix.
	static NodePtr &ChildRef(const ART &art, NodeHandle &handle) {
		return *reinterpret_cast<NodePtr *>(handle.GetPtr() + art.PrefixCount() + 1);
	}

	//! Get a mutable reference to the child slot using an explicit prefix byte count.
	static NodePtr &ChildRefWithCount(NodeHandle &handle, const idx_t count) {
		return ChildRefWithCount(handle.GetPtr(), count);
	}

	//! Get a mutable reference to the child slot using an explicit prefix byte count.
	static NodePtr &ChildRefWithCount(const data_ptr_t data, const idx_t count) {
		return *reinterpret_cast<NodePtr *>(data + count + 1);
	}

	//! Transform prefix chain to deprecated format.
	//! Returns an empty NodePtr if the prefix was not loaded from storage (early out) or if the endpoint
	//! was a gated node (handled internally). Otherwise, returns a copy of the child pointer at the tail of
	//! the prefix chain for further traversal.
	static NodePtr TransformToDeprecated(ART &art, NodePtr &node, TransformToDeprecatedState &state);

private:
	static NodeHandle TransformToDeprecatedAppend(NodeHandle handle, ART &art, FixedSizeAllocator &allocator,
	                                              const uint8_t byte);

private:
	NodeHandle handle;
};

} // namespace duckdb
