#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

PrefixHandle::PrefixHandle(const ART &art, const Node node)
    : segment_handle(make_uniq<SegmentHandle>(Node::GetAllocator(art, PREFIX).GetHandle(node))) {
	data = segment_handle->GetPtr();
	child = reinterpret_cast<Node *>(data + art.PrefixCount() + 1);
	segment_handle->MarkModified();
}

PrefixHandle::PrefixHandle(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node node, const uint8_t count)
    : segment_handle(make_uniq<SegmentHandle>(allocator->GetHandle(node))) {
	data = segment_handle->GetPtr();
	child = reinterpret_cast<Node *>(data + count + 1);
	segment_handle->MarkModified();
}

PrefixHandle::PrefixHandle(PrefixHandle &&other) noexcept
    : segment_handle(std::move(other.segment_handle)), data(other.data), child(other.child) {
	other.data = nullptr;
	other.child = nullptr;
}

PrefixHandle &PrefixHandle::operator=(PrefixHandle &&other) noexcept {
	if (this != &other) {
		segment_handle = std::move(other.segment_handle);
		data = other.data;
		child = other.child;

		other.data = nullptr;
		other.child = nullptr;
	}
	return *this;
}

PrefixHandle PrefixHandle::NewDeprecated(unsafe_unique_ptr<FixedSizeAllocator> &allocator, Node &node) {
	node = allocator->New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	PrefixHandle handle(allocator, node, DEPRECATED_COUNT);
	handle.data[DEPRECATED_COUNT] = 0;
	return handle;
}

void PrefixHandle::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET) {
			auto &alloc = Node::GetAllocator(art, PREFIX);
			if (!alloc.LoadedFromStorage(ref)) {
				return;
			}
			PrefixHandle handle(art, ref);
			ref = *handle.child;
		}
		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// We need to create a new prefix (chain) in the deprecated format.
	Node new_node;
	auto new_handle = PrefixHandle::NewDeprecated(allocator, new_node);

	Node current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		auto &alloc = Node::GetAllocator(art, PREFIX);
		if (!alloc.LoadedFromStorage(current_node)) {
			return;
		}

		PrefixHandle current_handle(art, current_node);

		for (idx_t i = 0; i < current_handle.data[art.PrefixCount()]; i++) {
			new_handle = new_handle.TransformToDeprecatedAppend(art, allocator, current_handle.data[i]);
		}

		*new_handle.child = *current_handle.child;
		Node::FreeNode(art, current_node);
		current_node = *new_handle.child;
	}

	node = new_node;
	return Node::TransformToDeprecated(art, *new_handle.child, allocator);
}

PrefixHandle PrefixHandle::TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator,
                                                       const uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return std::move(*this);
	}

	auto new_prefix = PrefixHandle::NewDeprecated(allocator, *child);
	return new_prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
