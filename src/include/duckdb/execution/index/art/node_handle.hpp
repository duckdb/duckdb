//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"

namespace duckdb {

class ART;
class NodePtr;
enum class NType : uint8_t;

//! NodeHandle is a mutable wrapper to access and modify a node.
//! A segment handle is used for memory management and marks memory as modified.
//! For read-only access, use ConstNodeHandle instead.
class NodeHandle {
public:
	NodeHandle(ART &art, const NodePtr node);
	NodeHandle(FixedSizeAllocator &allocator, const NodePtr node, NType type);

	NodeHandle(const NodeHandle &) = delete;
	NodeHandle &operator=(const NodeHandle &) = delete;
	NodeHandle(NodeHandle &&other) noexcept;
	NodeHandle &operator=(NodeHandle &&other) noexcept;

public:
	template <class T>
	T &Get() {
		D_ASSERT(T::TYPE == type);
		return handle.GetRef<T>();
	}

	data_ptr_t GetPtr() {
		return handle.GetPtr();
	}

	NType GetType() const {
		return type;
	}

private:
	SegmentHandle handle;
	NType type;
};

//! NodePtrHandle owns the pin for the node containing a mutable NodePtr storage location.
class NodePtrHandle {
	friend class PrefixHandle;

public:
	NodePtrHandle(const NodePtrHandle &) = delete;
	NodePtrHandle &operator=(const NodePtrHandle &) = delete;
	NodePtrHandle(NodePtrHandle &&) noexcept = default;
	NodePtrHandle &operator=(NodePtrHandle &&) noexcept = default;

public:
	//! The reference is valid while this handle owns the containing node's pin.
	NodePtr &Get() {
		D_ASSERT(handle.GetPtr());
		return node_ptr.get();
	}

private:
	//! node_ptr_p must be stored in the node pinned by handle_p.
	NodePtrHandle(NodePtr &node_ptr_p, NodeHandle &&handle_p) : node_ptr(node_ptr_p), handle(std::move(handle_p)) {
		D_ASSERT(handle.GetPtr());
	}

private:
	reference<NodePtr> node_ptr;
	NodeHandle handle;
};

//! ConstNodeHandle is a read-only wrapper to access a node.
//! A segment handle is used for memory management, but it is not marked as modified.
//! For mutable access, use NodeHandle instead.
class ConstNodeHandle {
public:
	ConstNodeHandle(const ART &art, const NodePtr node);

	ConstNodeHandle(const ConstNodeHandle &) = delete;
	ConstNodeHandle &operator=(const ConstNodeHandle &) = delete;
	ConstNodeHandle(ConstNodeHandle &&) = delete;
	ConstNodeHandle &operator=(ConstNodeHandle &&) = delete;

public:
	template <class T>
	const T &Get() const {
		D_ASSERT(T::TYPE == type);
		return handle.GetRef<T>();
	}

	const_data_ptr_t GetPtr() const {
		return handle.GetPtr<const data_t>();
	}

	NType GetType() const {
		return type;
	}

private:
	SegmentHandle handle;
	NType type;
};

} // namespace duckdb
