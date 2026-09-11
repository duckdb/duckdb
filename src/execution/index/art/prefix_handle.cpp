#include "duckdb/execution/index/art/prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

PrefixHandle PrefixHandle::NewInternal(ART &art, NodePtr &node, const_data_ptr_t data, const uint8_t count,
                                       const idx_t offset) {
	node = NodePtr::GetAllocator(art, PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	PrefixHandle prefix(NodeHandle(art, node));
	prefix.SetCount(art, count);
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.Data(), data + offset, count);
	}
	prefix.Child(art).Clear();
	return prefix;
}

PrefixChain PrefixHandle::New(ART &art, const ARTKey &key, const idx_t depth, const idx_t count) {
	D_ASSERT(count > 0);

	NodePtr root;
	auto first_count = UnsafeNumericCast<uint8_t>(MinValue<idx_t>(art.PrefixCount(), count));
	auto prefix = NewInternal(art, root, key.data, first_count, depth);
	auto tail = std::move(prefix).IntoChild(art);

	idx_t offset = first_count;
	while (offset < count) {
		auto this_count = UnsafeNumericCast<uint8_t>(MinValue<idx_t>(art.PrefixCount(), count - offset));
		auto next = NewInternal(art, tail.Get(), key.data, this_count, depth + offset);
		tail = std::move(next).IntoChild(art);

		offset += this_count;
	}
	return {root, std::move(tail)};
}

PrefixHandle PrefixHandle::AppendByte(ART &art, PrefixHandle prefix, const uint8_t byte) {
	const auto count = prefix.GetCount(art);
	if (count != art.PrefixCount()) {
		prefix.SetByte(count, byte);
		prefix.SetCount(art, UnsafeNumericCast<uint8_t>(count + 1));
		return prefix;
	}

	auto tail = std::move(prefix).IntoChild(art);
	return NewInternal(art, tail.Get(), &byte, 1, 0);
}

void PrefixHandle::Append(ART &art, PrefixHandle prefix, NodePtr other) {
	D_ASSERT(other.HasMetadata());

	while (other.GetType() == PREFIX) {
		if (other.GetGateStatus() == GateStatus::GATE_SET) {
			prefix.Child(art) = other;
			return;
		}

		NodePtr next;
		{
			PrefixHandle other_prefix(NodeHandle(art, other));
			const auto count = other_prefix.GetCount(art);
			for (idx_t i = 0; i < count; i++) {
				prefix = AppendByte(art, std::move(prefix), other_prefix.GetByte(i));
			}
			next = other_prefix.Child(art);
			prefix.Child(art) = next;
		}

		NodePtr::FreeNode(art, other);
		other = next;
	}
	prefix.Child(art) = other;
}

NodePtr PrefixHandle::Split(ART &art, NodePtr &node, NodePtr &replacement, const uint8_t pos) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == PREFIX);
	D_ASSERT(replacement.HasMetadata());

	NodePtr child;
	{
		PrefixHandle prefix(NodeHandle(art, node));
		const auto count = prefix.GetCount(art);
		D_ASSERT(pos < count);

		if (pos + 1 < count) {
			// The split is not at the last prefix byte.
			// After the caller attaches the returned child, we get:
			// [this prefix minus split byte, minus remaining bytes (omitted if pos == 0)] ->
			// [new node at split byte] --(split byte)-->
			// [child with remaining bytes, and possibly remaining prefix nodes].

			// Create a new prefix and
			// 1. copy the remaining bytes of this prefix.
			// 2. append remaining prefix nodes.
			const auto suffix_count = UnsafeNumericCast<uint8_t>(count - pos - 1);
			auto suffix = NewInternal(art, child, prefix.Data(), suffix_count, pos + 1);
			Append(art, std::move(suffix), prefix.Child(art));
		} else {
			// The split is at the last prefix byte, whether the prefix is full or not.
			// There are no bytes left in this prefix after the split.
			// After the caller attaches the returned child, we get:
			// [this prefix minus split byte (omitted if pos == 0)] ->
			// [new node at split byte] --(split byte)-->
			// [child at split byte: prefix.Child(art)].
			child = prefix.Child(art);
		}

		if (pos != 0) {
			// There are bytes left before the split.
			// The subsequent node replaces the split byte.
			// Any gate stays on this prefix.
			prefix.SetCount(art, pos);
			prefix.Child(art) = replacement;
			return child;
		}
		// No bytes left before the split, so replacement inherits this node's gate before we free it.
		replacement.SetGateStatus(node.GetGateStatus());
	}

	// Release the prefix handle before freeing its node, which may destroy the allocator buffer.
	NodePtr::FreeNode(art, node);
	node = replacement;
	return child;
}

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
