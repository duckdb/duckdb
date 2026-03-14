//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"

namespace duckdb {

class ART;
class NodePointer;
enum class NType : uint8_t;

class NodeHandle {
public:
	NodeHandle(ART &art, const NodePointer node);
	NodeHandle(FixedSizeAllocator &allocator, const NodePointer node, NType type);

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

class ConstNodeHandle {
public:
	ConstNodeHandle(const ART &art, const NodePointer node);

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
