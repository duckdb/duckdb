#include "duckdb/execution/index/art/node_handle.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"

namespace duckdb {

NodeHandle::NodeHandle(ART &art, const NodePtr node_ptr)
    : handle(NodePtr::GetAllocator(art, node_ptr.GetType()).GetHandle(node_ptr)), type(node_ptr.GetType()) {
	handle.MarkModified();
}

NodeHandle::NodeHandle(FixedSizeAllocator &allocator, const NodePtr node_ptr, NType type)
    : handle(allocator.GetHandle(node_ptr)), type(type) {
	D_ASSERT(node_ptr.GetType() == type);
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

ConstNodeHandle::ConstNodeHandle(const ART &art, const NodePtr node_ptr)
    : handle(NodePtr::GetAllocator(art, node_ptr.GetType()).GetHandle(node_ptr)), type(node_ptr.GetType()) {
}

NodePtrHandle::NodePtrHandle(NodePtr &node_ptr_p, optional<NodeHandle> handle_p)
    : node_ptr(node_ptr_p), handle(std::move(handle_p)) {
}

NodePtrHandle::NodePtrHandle(NodePtrHandle &&other) noexcept
    : node_ptr(other.node_ptr), handle(std::move(other.handle)) {
	other.Release();
}

NodePtrHandle &NodePtrHandle::operator=(NodePtrHandle &&other) noexcept {
	if (this != &other) {
		node_ptr = other.node_ptr;
		handle = std::move(other.handle);
		other.Release();
	}
	return *this;
}

NodePtrHandle NodePtrHandle::Unpinned(NodePtr &node_ptr) {
	return NodePtrHandle(node_ptr, nullopt);
}

NodePtrHandle NodePtrHandle::PrefixChild(const ART &art, NodeHandle &&prefix_handle) {
	D_ASSERT(prefix_handle.GetType() == NType::PREFIX);
	auto &child_ptr = PrefixHandle::ChildRef(art, prefix_handle);
	return NodePtrHandle(child_ptr, std::move(prefix_handle));
}

optional<NodePtrHandle> NodePtrHandle::Child(NodeHandle &&node_handle, const uint8_t byte) {
	auto child_ptr = NodePtr::GetChildMutable(node_handle, byte);
	if (!child_ptr) {
		return nullopt;
	}
	return NodePtrHandle(*child_ptr, std::move(node_handle));
}

NodePtr &NodePtrHandle::Get() {
	return *node_ptr;
}

void NodePtrHandle::Release() {
	node_ptr = nullptr;
	handle.reset();
}

NodeHandle NodePtrHandle::TakePin() && {
	if (!handle) {
		throw InternalException("NodePtrHandle::TakePin requires a pinned handle");
	}
	NodeHandle pin(std::move(*handle));
	Release();
	return pin;
}

} // namespace duckdb
