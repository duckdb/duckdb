#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

Prefix::Prefix(const ART &art, const Node ptr_p, const bool is_mutable, const bool set_in_memory) {
	if (!set_in_memory) {
		data = Node::GetAllocator(art, PREFIX).Get(ptr_p, is_mutable);
	} else {
		data = Node::GetAllocator(art, PREFIX).GetIfLoaded(ptr_p);
		if (!data) {
			ptr = nullptr;
			in_memory = false;
			return;
		}
	}
	ptr = reinterpret_cast<Node *>(data + Count(art) + 1);
	in_memory = true;
}

Prefix::Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count) {
	data = allocator->Get(ptr_p, true);
	ptr = reinterpret_cast<Node *>(data + count + 1);
	in_memory = true;
}

uint8_t Prefix::GetByte(const ART &art, const Node &node, const uint8_t pos) {
	D_ASSERT(node.GetType() == PREFIX);
	Prefix prefix(art, node);
	return prefix.data[pos];
}

Prefix Prefix::NewInternal(ART &art, Node &node, const data_ptr_t data, const uint8_t count, const idx_t offset) {
	node = Node::GetAllocator(art, PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(PREFIX));

	Prefix prefix(art, node, true);
	prefix.data[Count(art)] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	prefix.ptr->Clear();
	return prefix;
}

void Prefix::New(ART &art, reference<Node> &ref, const ARTKey &key, const idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(Count(art)), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, ref, key.data, this_count, offset + depth);

		ref = *prefix.ptr;
		offset += this_count;
		count -= this_count;
	}
}

void Prefix::Concat(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte,
                    const GateStatus node4_status) {
	// We have four situations from which we enter here:
	// 1: PREFIX (parent) - Node4 (prev_node4) - PREFIX (child) - INLINED_LEAF, or
	// 2: PREFIX (parent) - Node4 (prev_node4) - INLINED_LEAF (child), or
	// 3: Node (parent) - Node4 (prev_node4) - PREFIX (child) - INLINED_LEAF, or
	// 4: Node (parent) - Node4 (prev_node4) - INLINED_LEAF (child).

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

	auto inside_gate = parent.GetGateStatus() == GateStatus::GATE_SET;
	ConcatInternal(art, parent, node4, child, byte, inside_gate);
	return;
}

void Prefix::Reduce(ART &art, Node &node, const idx_t pos) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(pos < Count(art));

	// We always reduce by at least one byte,
	// thus, if the prefix was a gate, it no longer is.
	node.SetGateStatus(GateStatus::GATE_NOT_SET);

	Prefix prefix(art, node);
	if (pos == idx_t(prefix.data[Count(art)] - 1)) {
		auto next = *prefix.ptr;
		Node::FreeNode(art, node);
		node = next;
		return;
	}

	// FIXME: Copy into new prefix (chain) instead of shifting.
	for (idx_t i = 0; i < Count(art) - pos - 1; i++) {
		prefix.data[i] = prefix.data[pos + i + 1];
	}

	prefix.data[Count(art)] -= pos + 1;
	prefix.Append(art, *prefix.ptr);
}

GateStatus Prefix::Split(ART &art, reference<Node> &node, Node &child, const uint8_t pos) {
	D_ASSERT(node.get().HasMetadata());

	Prefix prefix(art, node, true);

	// The split is at the last prefix byte, and the prefix is full.
	// We decrease the count and return.
	// We get:
	// [this prefix minus its last byte] ->
	// [new node at split byte] ->
	// [child at split byte: prefix.ptr].
	if (pos + 1 == Count(art)) {
		prefix.data[Count(art)]--;
		node = *prefix.ptr;
		child = *prefix.ptr;
		return GateStatus::GATE_NOT_SET;
	}

	if (pos + 1 < prefix.data[Count(art)]) {
		// The split is not at the last prefix byte.
		// We get:
		// [this prefix minus split byte, minus remaining bytes] ->
		// [new node at split byte] ->
		// [child with remaining bytes, and possibly remaining prefix nodes].

		// Create a new prefix and
		// 1. copy the remaining bytes of this prefix.
		// 2. append remaining prefix nodes.
		auto new_prefix = NewInternal(art, child, nullptr, 0, 0);
		new_prefix.data[Count(art)] = prefix.data[Count(art)] - pos - 1;
		memcpy(new_prefix.data, prefix.data + pos + 1, new_prefix.data[Count(art)]);

		if (prefix.ptr->GetType() == PREFIX && prefix.ptr->GetGateStatus() == GateStatus::GATE_NOT_SET) {
			new_prefix.Append(art, *prefix.ptr);
		} else {
			*new_prefix.ptr = *prefix.ptr;
		}

	} else {
		D_ASSERT(pos + 1 == prefix.data[Count(art)]);
		// The split is at the last prefix byte, but the prefix is not full.
		// There are no other bytes or prefixes after the split.
		// We get:
		// [this prefix minus split byte (can be its only byte, then we free it)] ->
		// [new node at split byte] ->
		// [child at split byte: prefix.ptr].
		child = *prefix.ptr;
	}

	// Set the new count of this node (can be empty).
	prefix.data[Count(art)] = pos;

	// No bytes left before the split, free this node.
	if (pos == 0) {
		auto old_status = node.get().GetGateStatus();
		Node::FreeNode(art, node);
		return old_status;
	}

	// There are bytes left before the split.
	// The subsequent node replaces the split byte.
	node = *prefix.ptr;
	return GateStatus::GATE_NOT_SET;
}

string Prefix::ToString(ART &art, const Node &node, idx_t indent_level, bool inside_gate, bool display_ascii) {
	auto indent = [](string &str, const idx_t n) {
		for (idx_t i = 0; i < n; ++i) {
			str += " ";
		}
	};
	auto format_byte = [&](uint8_t byte) {
		if (!inside_gate && display_ascii && byte >= 32 && byte <= 126) {
			return string(1, static_cast<char>(byte));
		}
		return to_string(byte);
	};
	string str = "";
	indent(str, indent_level);
	reference<const Node> ref(node);
	Iterator(art, ref, true, false, [&](const Prefix &prefix) {
		str += "Prefix: |";
		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			str += format_byte(prefix.data[i]) + "|";
		}
	});

	auto child = ref.get().ToString(art, indent_level, inside_gate, display_ascii);
	return str + "\n" + child;
}

void Prefix::Verify(ART &art, const Node &node) {
	reference<const Node> ref(node);

	Iterator(art, ref, true, false, [&](Prefix &prefix) {
		D_ASSERT(prefix.data[Count(art)] != 0);
		D_ASSERT(prefix.data[Count(art)] <= Count(art));
	});

	ref.get().Verify(art);
}

void Prefix::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && ref.get().GetGateStatus() == GateStatus::GATE_NOT_SET) {
			Prefix prefix(art, ref, true, true);
			if (!prefix.in_memory) {
				return;
			}
			ref = *prefix.ptr;
		}
		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// We need to create a new prefix (chain).
	Node new_node;
	new_node = allocator->New();
	new_node.SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix new_prefix(allocator, new_node, DEPRECATED_COUNT);

	Node current_node = node;
	while (current_node.GetType() == PREFIX && current_node.GetGateStatus() == GateStatus::GATE_NOT_SET) {
		Prefix prefix(art, current_node, true, true);
		if (!prefix.in_memory) {
			return;
		}

		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			new_prefix = new_prefix.TransformToDeprecatedAppend(art, allocator, prefix.data[i]);
		}

		*new_prefix.ptr = *prefix.ptr;
		Node::FreeNode(art, current_node);
		current_node = *new_prefix.ptr;
	}

	node = new_node;
	return Node::TransformToDeprecated(art, *new_prefix.ptr, allocator);
}

Prefix Prefix::Append(ART &art, const uint8_t byte) {
	if (data[Count(art)] != Count(art)) {
		data[data[Count(art)]] = byte;
		data[Count(art)]++;
		return *this;
	}

	auto prefix = NewInternal(art, *ptr, nullptr, 0, 0);
	return prefix.Append(art, byte);
}

void Prefix::Append(ART &art, Node other) {
	D_ASSERT(other.HasMetadata());

	Prefix prefix = *this;
	while (other.GetType() == PREFIX) {
		if (other.GetGateStatus() == GateStatus::GATE_SET) {
			*prefix.ptr = other;
			return;
		}

		Prefix other_prefix(art, other, true);
		for (idx_t i = 0; i < other_prefix.data[Count(art)]; i++) {
			prefix = prefix.Append(art, other_prefix.data[i]);
		}

		*prefix.ptr = *other_prefix.ptr;
		Node::FreeNode(art, other);
		other = *prefix.ptr;
	}
}

Prefix Prefix::GetTail(ART &art, const Node &node) {
	Prefix prefix(art, node, true);
	while (prefix.ptr->GetType() == PREFIX) {
		prefix = Prefix(art, *prefix.ptr, true);
	}
	return prefix;
}

void Prefix::ConcatInternal(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte,
                            const bool inside_gate) {
	if (child.GetType() == NType::LEAF_INLINED) {
		if (inside_gate) {
			if (parent.GetType() == NType::PREFIX) {
				// The parent only contained the Node4, so we can now inline 'all the way up',
				// and the gate is no longer nested.
				while (parent.GetType() == NType::PREFIX) {
					Prefix prefix(art, parent, true);
					auto temp = *prefix.ptr;
					Node::FreeNode(art, parent);
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
			*tail.ptr = child;
			return;
		}

		auto prefix = NewInternal(art, node4, &byte, 1, 0);
		*prefix.ptr = child;
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
		*tail.ptr = child;
		return;
	}

	// The child is not inlined, and the parent is not a prefix.
	auto prefix = NewInternal(art, node4, &byte, 1, 0);
	if (child.GetType() == NType::PREFIX) {
		prefix.Append(art, child);
		return;
	}
	*prefix.ptr = child;
}

void Prefix::ConcatNode4WasGate(ART &art, Node &node4, const Node child, uint8_t byte) {
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
		prefix.ptr->Clear();
		prefix.Append(art, child);
		node4.SetGateStatus(GateStatus::GATE_SET);
		return;
	}
	// At least one more row ID in this gate and the child is not a prefix.
	// We create a new prefix of length one containing the remaining byte.
	// then, we append the child.
	auto prefix = NewInternal(art, node4, &byte, 1, 0);
	*prefix.ptr = child;
	node4.SetGateStatus(GateStatus::GATE_SET);
}

void Prefix::ConcatChildIsGate(ART &art, Node &parent, Node &node4, const Node child, uint8_t byte) {
	if (parent.GetType() != PREFIX) {
		// Create a new prefix at the former position of the Node4,
		// and point it to the gate.
		auto prefix = NewInternal(art, node4, &byte, 1, 0);
		*prefix.ptr = child;
		return;
	}

	// The parent is a prefix (chain), so we need to append the byte to its tail.
	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);
	*tail.ptr = child;
}

Prefix Prefix::TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator, uint8_t byte) {
	if (data[DEPRECATED_COUNT] != DEPRECATED_COUNT) {
		data[data[DEPRECATED_COUNT]] = byte;
		data[DEPRECATED_COUNT]++;
		return *this;
	}

	*ptr = allocator->New();
	ptr->SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix prefix(allocator, *ptr, DEPRECATED_COUNT);
	return prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
