#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

NodeHandle PrefixHandle::NewDeprecated(FixedSizeAllocator &allocator, NodePointer &node) {
	node = allocator.New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	NodeHandle handle(allocator, node, PREFIX);
	auto data = handle.GetPtr();
	data[DEPRECATED_COUNT] = 0;
	return handle;
}

NodePointer PrefixHandle::TransformToDeprecated(ART &art, NodePointer &node, TransformToDeprecatedState &state) {
	// Early-out, if we do not need any transformations.
	if (!state.HasAllocator()) {
		NodePointer current = node;
		auto &allocator = NodePointer::GetAllocator(art, PREFIX);
		while (current.GetType() == PREFIX && current.GetGateStatus() == GateStatus::GATE_NOT_SET) {
			if (!allocator.LoadedFromStorage(current)) {
				return NodePointer();
			}
			NodeHandle handle(art, current);
			auto child = reinterpret_cast<NodePointer *>(handle.GetPtr() + art.PrefixCount() + 1);
			current = *child;
			// Handle gated endpoints while the prefix is still pinned.
			if (current.HasMetadata() && current.GetGateStatus() == GateStatus::GATE_SET) {
				Leaf::TransformToDeprecated(art, *child);
				return NodePointer();
			}
		}
		return current;
	}

	// We need to create a new prefix (chain) in the deprecated format.
	auto &deprecated_allocator = state.GetAllocator();
	NodePointer new_node;
	auto new_handle = NewDeprecated(deprecated_allocator, new_node);

	auto &allocator = NodePointer::GetAllocator(art, PREFIX);
	NodePointer current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		if (!allocator.LoadedFromStorage(current_node)) {
			return NodePointer();
		}
		{
			// Decrease the readers on current_handle after moving all data over.
			NodeHandle current_handle(art, current_node);
			auto current_data = current_handle.GetPtr();
			auto current_child = reinterpret_cast<NodePointer *>(current_data + art.PrefixCount() + 1);

			for (idx_t i = 0; i < current_data[art.PrefixCount()]; i++) {
				new_handle =
				    TransformToDeprecatedAppend(std::move(new_handle), art, deprecated_allocator, current_data[i]);
			}
			auto new_child = reinterpret_cast<NodePointer *>(new_handle.GetPtr() + DEPRECATED_COUNT + 1);
			*new_child = *current_child;
		}

		// Freeing the node here can trigger a buffer removal (last segment on the buffer).
		// In that case, there cannot be any readers left on the buffer.
		NodePointer::FreeNode(art, current_node);
		auto new_child = reinterpret_cast<NodePointer *>(new_handle.GetPtr() + DEPRECATED_COUNT + 1);
		current_node = *new_child;
	}

	node = new_node;
	auto new_child = reinterpret_cast<NodePointer *>(new_handle.GetPtr() + DEPRECATED_COUNT + 1);
	// Handle gated endpoints while the new prefix is still pinned.
	NodePointer endpoint = *new_child;
	if (endpoint.HasMetadata() && endpoint.GetGateStatus() == GateStatus::GATE_SET) {
		Leaf::TransformToDeprecated(art, *new_child);
		return NodePointer();
	}
	return endpoint;
}

NodeHandle PrefixHandle::TransformToDeprecatedAppend(NodeHandle handle, ART &art, FixedSizeAllocator &allocator,
                                                     const uint8_t byte) {
	auto data = handle.GetPtr();
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return handle;
	}

	auto child = reinterpret_cast<NodePointer *>(data + DEPRECATED_COUNT + 1);
	auto new_prefix = NewDeprecated(allocator, *child);
	return TransformToDeprecatedAppend(std::move(new_prefix), art, allocator, byte);
}

} // namespace duckdb
