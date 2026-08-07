//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"

namespace duckdb {

class ART;
class NodePtr;
enum class NType : uint8_t;

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

//! SlotHandle wraps a mutable reference to a NodePtr slot.
//! Ref() is valid for the lifetime of this object.
//! If constructed with a pin, SlotHandle owns the NodeHandle keeping the slot valid.
//! If constructed without a pin, the caller must guarantee the slot remains valid
class SlotHandle {
public:
	//! Create a slot handle for memory that is valid without this object owning a pin.
	explicit SlotHandle(NodePtr &slot_p) : slot(&slot_p) {
	}

	//! Create a slot handle that owns the pin keeping slot_p valid.
	SlotHandle(NodePtr &slot_p, NodeHandle &&pin_p) : slot(&slot_p), pin(std::move(pin_p)) {
	}

	SlotHandle(const SlotHandle &) = delete;
	SlotHandle &operator=(const SlotHandle &) = delete;
	SlotHandle(SlotHandle &&) = default;
	SlotHandle &operator=(SlotHandle &&) = default;

public:
	//! Returns the mutable slot.
	NodePtr &Ref() {
		D_ASSERT(slot);
		return *slot;
	}

	//! Rebind to memory valid without this object owning a pin, dropping the previous pin if there was one.
	void Rebind(NodePtr &slot_p) {
		slot = &slot_p;
		pin.reset();
	}

	//! Rebind to a slot protected by pin_p, dropping the previous pin if there was one.
	void Rebind(NodePtr &slot_p, NodeHandle &&pin_p) {
		slot = &slot_p;
		pin.emplace(std::move(pin_p));
	}

private:
	NodePtr *slot;
	optional<NodeHandle> pin;
};

class ConstNodeHandle {
public:
	ConstNodeHandle(const ART &art, const NodePtr node);

	ConstNodeHandle(const ConstNodeHandle &) = delete;
	ConstNodeHandle &operator=(const ConstNodeHandle &) = delete;
	ConstNodeHandle(ConstNodeHandle &&) = delete;
	ConstNodeHandle &operator=(ConstNodeHandle &&) = delete;

public:
	template <class T>
	const T &Get() {
		return handle.GetRef<T>();
	}

	const_data_ptr_t GetPtr() {
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
