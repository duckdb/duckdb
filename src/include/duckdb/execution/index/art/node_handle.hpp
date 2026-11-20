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
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
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
	NodeHandle(ART &art, const NodePtr node_ptr);
	NodeHandle(FixedSizeAllocator &allocator, const NodePtr node_ptr, NType type);

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

//! ConstNodeHandle is a read-only wrapper to access a node.
//! A segment handle is used for memory management, but it is not marked as modified.
//! For mutable access, use NodeHandle instead.
class ConstNodeHandle {
public:
	ConstNodeHandle(const ART &art, const NodePtr node_ptr);

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

//! NodePtrHandle refers to a mutable NodePtr and owns the pin on the node storing it.
//! NodeHandle pins the node that a NodePtr points to; NodePtrHandle pins the node that contains the NodePtr.
//! Use it only to pass a NodePtr to a callee that may free the node storing it, as in Delete: the callee releases
//! the pin before that free. Otherwise, keep a NodeHandle in scope and use a NodePtr reference, as Insert does.
//! Only ARTOperator can create an unpinned NodePtrHandle, for a NodePtr that no node stores, e.g., the ART root.
class NodePtrHandle {
	friend class ARTOperator;

public:
	NodePtrHandle(const NodePtrHandle &) = delete;
	NodePtrHandle &operator=(const NodePtrHandle &) = delete;
	NodePtrHandle(NodePtrHandle &&other) noexcept;
	NodePtrHandle &operator=(NodePtrHandle &&other) noexcept;

public:
	//! Refers to the child NodePtr of the pinned prefix.
	static NodePtrHandle PrefixChild(const ART &art, NodeHandle &&prefix_handle);
	//! Refers to the NodePtr of the child at byte in the pinned inner node, if it exists.
	static optional<NodePtrHandle> Child(NodeHandle &&node_handle, const uint8_t byte);

	//! The reference is valid until the handle is released, moved from, or destroyed.
	NodePtr &Get();
	//! Releases the pin. Afterwards, the handle no longer refers to a NodePtr.
	void Release();
	//! Moves the pin out of a pinned handle and releases the handle.
	NodeHandle TakePin() &&;

private:
	//! Refers to a NodePtr that no node stores. The caller keeps its storage valid.
	static NodePtrHandle Unpinned(NodePtr &node_ptr);

	NodePtrHandle(NodePtr &node_ptr_p, optional<NodeHandle> handle_p);

private:
	optional_ptr<NodePtr> node_ptr;
	optional<NodeHandle> handle;
};

} // namespace duckdb
