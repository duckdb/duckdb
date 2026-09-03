#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"

namespace duckdb {

Prefix::Prefix(const ART &art, const NodePtr node, const bool is_mutable, const bool set_in_memory) {
	if (!set_in_memory) {
		data = NodePtr::GetAllocator(art, PREFIX).Get(node, is_mutable);
	} else {
		data = NodePtr::GetAllocator(art, PREFIX).GetIfLoaded(node);
		if (!data) {
			child_slot = nullptr;
			in_memory = false;
			return;
		}
	}
	child_slot = &PrefixHandle::ChildRefWithCount(data, art.PrefixCount());
	in_memory = true;
}

Prefix::Prefix(FixedSizeAllocator &allocator, const NodePtr node, const idx_t count) {
	data = allocator.Get(node, true);
	child_slot = &PrefixHandle::ChildRefWithCount(data, count);
	in_memory = true;
}

uint8_t Prefix::GetByte(const ART &art, const NodePtr &node, const uint8_t pos) {
	D_ASSERT(node.GetType() == PREFIX);
	Prefix prefix(art, node);
	return prefix.data[pos];
}

Prefix Prefix::NewInternal(ART &art, NodePtr &node, const data_ptr_t data, const uint8_t count, const idx_t offset) {
	node = NodePtr::GetAllocator(art, PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	Prefix prefix(art, node, true);
	prefix.data[art.PrefixCount()] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	prefix.child_slot->Clear();
	return prefix;
}

void Prefix::New(ART &art, reference<NodePtr> &node_ref, const ARTKey &key, const idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(art.PrefixCount()), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, node_ref, key.data, this_count, offset + depth);

		node_ref = *prefix.child_slot;
		offset += this_count;
		count -= this_count;
	}
}

void Prefix::Concat(ART &art, NodePtr &parent, NodePtr &node4, const NodePtr child, uint8_t byte,
                    const GateStatus node4_status, const GateStatus status) {
	// We have four situations from which we enter here:
	// 1: PREFIX (parent) - Node4 (prev_node4) - PREFIX (child) - INLINED_LEAF, or
	// 2: PREFIX (parent) - Node4 (prev_node4) - INLINED_LEAF (child), or
	// 3: INTERNAL_NODE (parent) - Node4 (prev_node4) - PREFIX (child) - INLINED_LEAF, or
	// 4: INTERNAL_NODE (parent) - Node4 (prev_node4) - INLINED_LEAF (child).

	D_ASSERT(!parent.IsAnyLeaf());
	D_ASSERT(child.HasMetadata());

	// The Node4 was a gate.
	if (node4_status == GateStatus::GATE_SET) {
		D_ASSERT(parent.GetGateStatus() == GateStatus::GATE_NOT_SET);
		D_ASSERT(child.GetGateStatus() == GateStatus::GATE_NOT_SET);
		ConcatNode4WasGate(art, node4, child, byte);
		return;
	}

	// The child is a gate.
	if (child.GetGateStatus() == GateStatus::GATE_SET) {
		D_ASSERT(node4_status == GateStatus::GATE_NOT_SET);
		ConcatChildIsGate(art, parent, node4, child, byte);
		return;
	}
	ConcatInternal(art, parent, node4, child, byte, status);
}

void Prefix::Reduce(ART &art, NodePtr &node, const idx_t pos) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(pos < art.PrefixCount());

	// We always reduce by at least one byte,
	// thus, if the prefix was a gate, it no longer is.
	node.SetGateStatus(GateStatus::GATE_NOT_SET);

	Prefix prefix(art, node);
	if (pos == idx_t(prefix.data[art.PrefixCount()] - 1)) {
		auto next = *prefix.child_slot;
		NodePtr::FreeNode(art, node);
		node = next;
		return;
	}

	// FIXME: Copy into new prefix (chain) instead of shifting.
	for (idx_t i = 0; i < art.PrefixCount() - pos - 1; i++) {
		prefix.data[i] = prefix.data[pos + i + 1];
	}

	prefix.data[art.PrefixCount()] -= pos + 1;
	prefix.Append(art, *prefix.child_slot);
}

GateStatus Prefix::Split(ART &art, reference<NodePtr> &node_ref, NodePtr &child, const uint8_t pos) {
	D_ASSERT(node_ref.get().HasMetadata());

	Prefix prefix(art, node_ref, true);

	// The split is at the last prefix byte, and the prefix is full.
	// We decrease the count and return.
	// We get:
	// [this prefix minus its last byte] ->
	// [new node at split byte] ->
	// [child at split byte: prefix.child_slot].
	if (pos + 1 == art.PrefixCount()) {
		prefix.data[art.PrefixCount()]--;
		node_ref = *prefix.child_slot;
		child = *prefix.child_slot;
		return GateStatus::GATE_NOT_SET;
	}

	if (pos + 1 < prefix.data[art.PrefixCount()]) {
		// The split is not at the last prefix byte.
		// We get:
		// [this prefix minus split byte, minus remaining bytes] ->
		// [new node at split byte] ->
		// [child with remaining bytes, and possibly remaining prefix nodes].

		// Create a new prefix and
		// 1. copy the remaining bytes of this prefix.
		// 2. append remaining prefix nodes.
		auto new_prefix = NewInternal(art, child, nullptr, 0, 0);
		new_prefix.data[art.PrefixCount()] = prefix.data[art.PrefixCount()] - pos - 1;
		memcpy(new_prefix.data, prefix.data + pos + 1, new_prefix.data[art.PrefixCount()]);

		if (prefix.child_slot->GetType() == PREFIX && prefix.child_slot->GetGateStatus() == GateStatus::GATE_NOT_SET) {
			new_prefix.Append(art, *prefix.child_slot);
		} else {
			*new_prefix.child_slot = *prefix.child_slot;
		}

	} else {
		D_ASSERT(pos + 1 == prefix.data[art.PrefixCount()]);
		// The split is at the last prefix byte, but the prefix is not full.
		// There are no other bytes or prefixes after the split.
		// We get:
		// [this prefix minus split byte (can be its only byte, then we free it)] ->
		// [new node at split byte] ->
		// [child at split byte: prefix.child_slot].
		child = *prefix.child_slot;
	}

	// Set the new count of this node (can be empty).
	prefix.data[art.PrefixCount()] = pos;

	// No bytes left before the split, free this node.
	if (pos == 0) {
		auto old_status = node_ref.get().GetGateStatus();
		NodePtr::FreeNode(art, node_ref);
		return old_status;
	}

	// There are bytes left before the split.
	// The subsequent node replaces the split byte.
	node_ref = *prefix.child_slot;
	return GateStatus::GATE_NOT_SET;
}

Prefix Prefix::Append(ART &art, const uint8_t byte) {
	if (data[art.PrefixCount()] != art.PrefixCount()) {
		data[data[art.PrefixCount()]] = byte;
		data[art.PrefixCount()]++;
		return *this;
	}

	auto prefix = NewInternal(art, *child_slot, nullptr, 0, 0);
	return prefix.Append(art, byte);
}

void Prefix::Append(ART &art, NodePtr other) {
	D_ASSERT(other.HasMetadata());

	Prefix prefix = *this;
	while (other.GetType() == PREFIX) {
		if (other.GetGateStatus() == GateStatus::GATE_SET) {
			*prefix.child_slot = other;
			return;
		}

		Prefix other_prefix(art, other, true);
		for (idx_t i = 0; i < other_prefix.data[art.PrefixCount()]; i++) {
			prefix = prefix.Append(art, other_prefix.data[i]);
		}

		*prefix.child_slot = *other_prefix.child_slot;
		NodePtr::FreeNode(art, other);
		other = *prefix.child_slot;
	}
}

Prefix Prefix::GetTail(ART &art, const NodePtr &node) {
	Prefix prefix(art, node, true);
	while (prefix.child_slot->GetType() == PREFIX) {
		prefix = Prefix(art, *prefix.child_slot, true);
	}
	return prefix;
}

void Prefix::ConcatInternal(ART &art, NodePtr &parent, NodePtr &node4, const NodePtr child, uint8_t byte,
                            const GateStatus status) {
	if (child.GetType() == NType::LEAF_INLINED) {
		if (status == GateStatus::GATE_SET) {
			if (parent.GetType() == NType::PREFIX) {
				// The parent only contained the Node4, so we can now inline 'all the way up',
				// and the gate is no longer nested.
				while (parent.GetType() == NType::PREFIX) {
					Prefix prefix(art, parent, true);
					auto temp = *prefix.child_slot;
					NodePtr::FreeNode(art, parent);
					parent = temp;
				}
				parent = child;
				return;
			}
			// The parent is any node inside the gate.
			// Inside gates, inlined row IDs are not prefixed,
			// so we directly inline into the previous Node4.
			node4 = child;
			return;
		}

		// Not inside a gate.
		if (parent.GetType() == NType::PREFIX) {
			// Append the byte to the prefix, and then inline the child.
			auto tail = GetTail(art, parent);
			tail = tail.Append(art, byte);
			*tail.child_slot = child;
			return;
		}

		auto prefix = NewInternal(art, node4, &byte, 1, 0);
		*prefix.child_slot = child;
		return;
	}

	// The child is not inlined.
	if (parent.GetType() == NType::PREFIX) {
		// Append the byte to the prefix.
		auto tail = GetTail(art, parent);
		tail = tail.Append(art, byte);

		// Append the child to the prefix.
		if (child.GetType() == NType::PREFIX) {
			tail.Append(art, child);
			return;
		}
		*tail.child_slot = child;
		return;
	}

	// The child is not inlined, and the parent is not a prefix.
	auto prefix = NewInternal(art, node4, &byte, 1, 0);
	if (child.GetType() == NType::PREFIX) {
		prefix.Append(art, child);
		return;
	}
	*prefix.child_slot = child;
}

void Prefix::ConcatNode4WasGate(ART &art, NodePtr &node4, const NodePtr child, uint8_t byte) {
	D_ASSERT(child.HasMetadata());

	if (child.GetType() == NType::LEAF_INLINED) {
		// Inside gates, inlined row IDs are not prefixed.
		// The child is inlined, so we inline into the previous Node4.
		// There is no longer a nested leaf.
		node4 = child;
		return;
	}

	if (child.GetType() == PREFIX) {
		// At least one more row ID in this gate and the child is a prefix.
		// We create a new prefix of length one containing the remaining byte.
		// Then, we append the child prefix.
		auto prefix = NewInternal(art, node4, &byte, 1, 0);
		prefix.child_slot->Clear();
		prefix.Append(art, child);
		node4.SetGateStatus(GateStatus::GATE_SET);
		return;
	}
	// At least one more row ID in this gate and the child is not a prefix.
	// We create a new prefix of length one containing the remaining byte.
	// then, we append the child.
	auto prefix = NewInternal(art, node4, &byte, 1, 0);
	*prefix.child_slot = child;
	node4.SetGateStatus(GateStatus::GATE_SET);
}

void Prefix::ConcatChildIsGate(ART &art, NodePtr &parent, NodePtr &node4, const NodePtr child, uint8_t byte) {
	if (parent.GetType() != PREFIX) {
		// Create a new prefix at the former position of the Node4,
		// and point it to the gate.
		auto prefix = NewInternal(art, node4, &byte, 1, 0);
		*prefix.child_slot = child;
		return;
	}

	// The parent is a prefix (chain), so we need to append the byte to its tail.
	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);
	*tail.child_slot = child;
}

Prefix Prefix::TransformToDeprecatedAppend(ART &art, FixedSizeAllocator &allocator, uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return *this;
	}

	*child_slot = allocator.New();
	child_slot->SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix prefix(allocator, *child_slot, DEPRECATED_COUNT);
	return prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
