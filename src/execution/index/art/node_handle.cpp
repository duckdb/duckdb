#include "duckdb/execution/index/art/node_handle.hpp"

#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

NodeHandle::NodeHandle(ART &art, const NodePointer node)
    : handle(NodePointer::GetAllocator(art, node.GetType()).GetHandle(node)), type(node.GetType()) {
	handle.MarkModified();
}

NodeHandle::NodeHandle(FixedSizeAllocator &allocator, const NodePointer node, NType type)
    : handle(allocator.GetHandle(node)), type(type) {
	handle.MarkModified();
}

NodeHandle::NodeHandle(NodeHandle &&other) noexcept : handle(std::move(other.handle)), type(other.type) {
}

NodeHandle &NodeHandle::operator=(NodeHandle &&other) noexcept {
	if (this != &other) {
		handle = std::move(other.handle);
		type = other.type;
	}
	return *this;
}

ConstNodeHandle::ConstNodeHandle(const ART &art, const NodePointer node)
    : handle(NodePointer::GetAllocator(art, node.GetType()).GetHandle(node)), type(node.GetType()) {
}

} // namespace duckdb
