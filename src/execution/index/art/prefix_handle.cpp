#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

NodeHandle PrefixHandle::NewDeprecated(FixedSizeAllocator &allocator, NodePtr &node) {
	node = allocator.New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	NodeHandle handle(allocator, node, PREFIX);
	auto data = handle.GetPtr();
	data[DEPRECATED_COUNT] = 0;
	return handle;
}

OptionalNodePtr PrefixHandle::TransformToDeprecated(ART &art, NodePtr &node, TransformToDeprecatedState &state) {
	// Early-out, if we do not need any transformations.
	if (!state.HasAllocator()) {
		NodePtr current = node;
		auto &allocator = NodePtr::GetAllocator(art, PREFIX);
		while (current.GetType() == PREFIX && current.GetGateStatus() == GateStatus::GATE_NOT_SET) {
			if (!allocator.LoadedFromStorage(current)) {
				return OptionalNodePtr();
			}
			NodeHandle handle(art, current);
			auto &child = ChildRef(art, handle);
			current = child;
			// Handle gated endpoints while the parent of the prefix chain is still pinned.
			if (current.HasMetadata() && current.GetGateStatus() == GateStatus::GATE_SET) {
				Leaf::TransformToDeprecated(art, child);
				return OptionalNodePtr();
			}
		}
		return current;
	}

	// We need to create a new prefix (chain) in the deprecated format.
	auto &deprecated_allocator = state.GetAllocator();
	NodePtr rebuilt_prefix;
	auto tail_handle = NewDeprecated(deprecated_allocator, rebuilt_prefix);

	auto &allocator = NodePtr::GetAllocator(art, PREFIX);
	NodePtr source_prefix = node;
	while (source_prefix.GetType() == PREFIX && source_prefix.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		if (!allocator.LoadedFromStorage(source_prefix)) {
			return OptionalNodePtr();
		}
		{
			// Decrease the readers on source_handle after moving all data over.
			NodeHandle source_handle(art, source_prefix);
			auto source_data = source_handle.GetPtr();
			auto &source_child = ChildRef(art, source_handle);

			for (idx_t i = 0; i < source_data[art.PrefixCount()]; i++) {
				tail_handle =
				    TransformToDeprecatedAppend(std::move(tail_handle), art, deprecated_allocator, source_data[i]);
			}
			auto &tail_child = ChildRefWithCount(tail_handle, DEPRECATED_COUNT);
			tail_child = source_child;
		}

		// Freeing the node here can trigger a buffer removal (last segment on the buffer).
		// In that case, there cannot be any readers left on the buffer.
		NodePtr::FreeNode(art, source_prefix);
		auto &tail_child = ChildRefWithCount(tail_handle, DEPRECATED_COUNT);
		source_prefix = tail_child;
	}

	node = rebuilt_prefix;
	auto &tail_child = ChildRefWithCount(tail_handle, DEPRECATED_COUNT);
	// Handle gated endpoints while the new prefix is still pinned.
	NodePtr endpoint = tail_child;
	if (endpoint.HasMetadata() && endpoint.GetGateStatus() == GateStatus::GATE_SET) {
		Leaf::TransformToDeprecated(art, tail_child);
		return OptionalNodePtr();
	}
	return endpoint;
}

NodeHandle PrefixHandle::TransformToDeprecatedAppend(NodeHandle tail_handle, ART &art, FixedSizeAllocator &allocator,
                                                     const uint8_t byte) {
	auto tail_data = tail_handle.GetPtr();
	if (tail_data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		tail_data[tail_data[DEPRECATED_COUNT]] = byte;
		tail_data[DEPRECATED_COUNT]++;
		return tail_handle;
	}

	auto &tail_child = ChildRefWithCount(tail_data, DEPRECATED_COUNT);
	auto new_tail_handle = NewDeprecated(allocator, tail_child);
	return TransformToDeprecatedAppend(std::move(new_tail_handle), art, allocator, byte);
}

} // namespace duckdb
