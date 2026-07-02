#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

PrefixHandle::PrefixHandle(const ART &art, const Node node)
    : segment_handle(Node::GetAllocator(art, PREFIX).GetHandle(node)) {
	data = segment_handle.GetPtr();
	child = reinterpret_cast<Node *>(data + art.PrefixCount() + 1);
	segment_handle.MarkModified();
}

PrefixHandle::PrefixHandle(FixedSizeAllocator &allocator, const Node node, const uint8_t count)
    : segment_handle(allocator.GetHandle(node)) {
	data = segment_handle.GetPtr();
	child = reinterpret_cast<Node *>(data + count + 1);
	segment_handle.MarkModified();
}

PrefixHandle::PrefixHandle(PrefixHandle &&other) noexcept
    : data(other.data), child(other.child), segment_handle(std::move(other.segment_handle)) {
	other.data = nullptr;
	other.child = nullptr;
}

PrefixHandle &PrefixHandle::operator=(PrefixHandle &&other) noexcept {
	if (this != &other) {
		// old segment handle reader count gets decremented on this move.
		segment_handle = std::move(other.segment_handle);

		data = other.data;
		child = other.child;

		other.data = nullptr;
		other.child = nullptr;
	}
	return *this;
}

PrefixHandle PrefixHandle::NewDeprecated(FixedSizeAllocator &allocator, Node &node) {
	node = allocator.New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	PrefixHandle handle(allocator, node, DEPRECATED_COUNT);
	handle.data[DEPRECATED_COUNT] = 0;
	return handle;
}

optional_ptr<Node> PrefixHandle::TransformToDeprecated(ART &art, Node &node, TransformToDeprecatedState &state) {
	// Early-out, if we do not need any transformations.
	if (!state.HasAllocator()) {
		reference<Node> ref(node);
		auto &allocator = Node::GetAllocator(art, PREFIX);
		while (ref.get().GetType() == PREFIX && ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET) {
			if (!allocator.LoadedFromStorage(ref)) {
				return nullptr;
			}
			PrefixHandle handle(art, ref);
			ref = *handle.child;
		}
		return ref.get();
	}

	// We need to create a new prefix (chain) in the deprecated format.
	auto &deprecated_allocator = state.GetAllocator();
	Node new_node;
	auto new_handle = NewDeprecated(deprecated_allocator, new_node);

	auto &allocator = Node::GetAllocator(art, PREFIX);
	Node current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		if (!allocator.LoadedFromStorage(current_node)) {
			return nullptr;
		}
		{
			// Decrease the readers on current_handle after moving all data over.
			PrefixHandle current_handle(art, current_node);
			for (idx_t i = 0; i < current_handle.data[art.PrefixCount()]; i++) {
				new_handle = new_handle.TransformToDeprecatedAppend(art, deprecated_allocator, current_handle.data[i]);
			}
			*new_handle.child = *current_handle.child;
		}

		// Freeing the node here can trigger a buffer removal (last segment on the buffer).
		// In that case, there cannot be any readers left on the buffer.
		Node::FreeNode(art, current_node);
		current_node = *new_handle.child;
	}

	node = new_node;
	return new_handle.child;
}

PrefixHandle PrefixHandle::TransformToDeprecatedAppend(ART &art, FixedSizeAllocator &allocator, const uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return std::move(*this);
	}

	auto new_prefix = NewDeprecated(allocator, *child);
	return new_prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
