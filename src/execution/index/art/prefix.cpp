#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/swap.hpp"

namespace duckdb {

Prefix::Prefix(const ART &art, const Node ptr_p, const bool is_mutable, const bool set_in_memory) {
	auto type = ptr_p.GetType();
	if (!set_in_memory) {
		data = Node::GetAllocator(art, type).Get(ptr_p, is_mutable);
	} else {
		data = Node::GetAllocator(art, type).GetInMemoryPtr(ptr_p);
		if (!data) {
			ptr = nullptr;
			in_memory = false;
			return;
		}
	}
	ptr = type == INLINED ? nullptr : reinterpret_cast<Node *>(data + Size(art));
	in_memory = true;
}

Prefix::Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count) {
	data = allocator->Get(ptr_p, true);
	ptr = reinterpret_cast<Node *>(data + count + 1);
	in_memory = true;
}

void Prefix::NewInlined(ART &art, Node &node, const ARTKey &key, idx_t depth, uint8_t count) {
	D_ASSERT(count <= art.prefix_count);
	NewInternal(art, node, key.data, count, depth, INLINED);
}

void Prefix::New(ART &art, reference<Node> &node, const ARTKey &key, idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(Count(art)), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, node, key.data, this_count, offset + depth, PREFIX);

		node = *prefix.ptr;
		offset += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {
	Node next;
	while (node.HasMetadata() && node.IsPrefix()) {
		D_ASSERT(node.GetType() != INLINED);
		Prefix prefix(art, node, true);
		next = *prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(node);
		node = next;
	}

	Node::Free(art, node);
	node.Clear();
}

void Prefix::InitializeMerge(ART &art, Node &node, const unsafe_vector<idx_t> &upper_bounds) {
	D_ASSERT(node.GetType() != INLINED);
	auto allocator_idx = Node::GetAllocatorIdx(PREFIX);
	auto buffer_count = upper_bounds[allocator_idx];

	Node next_node = node;
	Prefix prefix(art, next_node, true);

	while (next_node.GetType() == PREFIX) {
		next_node = *prefix.ptr;
		if (prefix.ptr->GetType() == PREFIX) {
			prefix.ptr->IncreaseBufferId(buffer_count);
			prefix = Prefix(art, next_node, true);
		}
	}

	node.IncreaseBufferId(buffer_count);
	prefix.ptr->InitializeMerge(art, upper_bounds);
}

row_t Prefix::CanInline(ART &art, Node &parent, Node &node, uint8_t byte, const Node &child) {
	if (art.IsUnique()) {
		return -1;
	}
	if (node.GetType() != NType::NODE_7_LEAF && child.GetType() != INLINED) {
		return -1;
	}

	idx_t concat_size = 0;
	uint8_t data[sizeof(row_t)];
	if (parent.GetType() == PREFIX) {
		Prefix prefix(art, parent);
		concat_size += prefix.data[art.prefix_count];
		memcpy(data, prefix.data, prefix.data[art.prefix_count]);
	}
	data[concat_size] = byte;
	concat_size++;

	if (node.GetType() == NType::NODE_7_LEAF) {
		if (concat_size < sizeof(row_t)) {
			return -1;
		} else {
			return ARTKey(&data[0], sizeof(row_t)).GetRowID();
		}
	}

	Prefix child_prefix(art, child);
	memcpy(data + concat_size, child_prefix.data, child_prefix.data[art.prefix_count]);
	concat_size += child_prefix.data[art.prefix_count];
	if (concat_size < sizeof(row_t)) {
		return -1;
	}
	return ARTKey(&data[0], sizeof(row_t)).GetRowID();
}

void Prefix::Concat(ART &art, Node &parent, uint8_t byte, bool is_gate, const Node &child) {
	D_ASSERT(parent.HasMetadata());
	D_ASSERT(!parent.IsAnyLeaf());

	if (is_gate) {
		return ConcatGate(art, parent, byte, child);
	}
	if (child.IsGate()) {
		return ConcatChildIsGate(art, parent, byte, child);
	}

	if (!child.HasMetadata() || child.GetType() == INLINED) {
		return ConcatInlinedPrefix(art, parent, byte, child);
	}

	if (parent.GetType() != PREFIX) {
		auto prefix = NewInternal(art, parent, &byte, 1, 0, PREFIX);
		if (child.GetType() == PREFIX) {
			prefix.Append(art, child);
		} else {
			*prefix.ptr = child;
		}
		return;
	}

	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);

	if (child.GetType() == PREFIX) {
		tail.Append(art, child);
	} else {
		*tail.ptr = child;
	}
}

template <class NODE>
idx_t TraverseInternal(ART &art, reference<NODE> &node, const ARTKey &key, idx_t &depth,
                       const bool is_mutable = false) {

	D_ASSERT(node.get().HasMetadata());
	D_ASSERT(node.get().GetType() == NType::PREFIX);

	while (node.get().GetType() == NType::PREFIX) {
		Prefix prefix(art, node, is_mutable);
		for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		node = *prefix.ptr;
		D_ASSERT(node.get().HasMetadata());
		if (node.get().IsGate()) {
			break;
		}
	}

	return DConstants::INVALID_INDEX;
}

idx_t Prefix::Traverse(ART &art, reference<const Node> &node, const ARTKey &key, idx_t &depth) {
	return TraverseInternal<const Node>(art, node, key, depth);
}

idx_t Prefix::TraverseMutable(ART &art, reference<Node> &node, const ARTKey &key, idx_t &depth) {
	return TraverseInternal<Node>(art, node, key, depth, true);
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &pos, const bool in_gate) {
	D_ASSERT(l_node.get().HasMetadata());
	D_ASSERT(r_node.get().HasMetadata());

	if (l_node.get().GetType() == INLINED) {
		return TraverseInlined(art, l_node, r_node, pos);
	}

	Prefix l_prefix(art, l_node, true);
	Prefix r_prefix(art, r_node, true);

	idx_t max_count = MinValue(l_prefix.data[Count(art)], r_prefix.data[Count(art)]);
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			pos = i;
			break;
		}
	}
	if (pos != DConstants::INVALID_INDEX) {
		return true;
	}

	// Match.
	if (l_prefix.data[Count(art)] == r_prefix.data[Count(art)]) {
		return l_prefix.ptr->MergeInternal(art, *r_prefix.ptr, in_gate);
	}

	pos = max_count;
	if (r_prefix.ptr->GetType() != PREFIX && r_prefix.data[Count(art)] == max_count) {
		// l_prefix contains r_prefix
		swap(l_node.get(), r_node.get());
		l_node = *r_prefix.ptr;
	} else {
		// r_prefix contains l_prefix
		l_node = *l_prefix.ptr;
	}
	return true;
}

uint8_t Prefix::GetByte(const ART &art, const Node &node, uint8_t pos) {
	D_ASSERT(node.IsPrefix());
	Prefix prefix(art, node);
	return prefix.data[pos];
}

void Prefix::Reduce(ART &art, Node &node, const idx_t n) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(n < Count(art));

	switch (node.GetType()) {
	case INLINED: {
		return ReduceInlinedPrefix(art, node, n);
	}
	case PREFIX: {
		return ReducePrefix(art, node, n);
	}
	default:
		throw InternalException("Invalid prefix type for Reduce.");
	}
}

bool Prefix::Split(ART &art, reference<Node> &node, Node &child, uint8_t pos) {
	D_ASSERT(node.get().HasMetadata());
	if (node.get().GetType() == INLINED) {
		return SplitInlined(art, node, child, pos);
	}

	Prefix prefix(art, node);

	// The split is at the last prefix byte. Decrease the count and return.
	if (pos + 1 == Count(art)) {
		prefix.data[Count(art)]--;
		node = *prefix.ptr;
		child = *prefix.ptr;
		return false;
	}

	if (pos + 1 < prefix.data[Count(art)]) {
		// Create a new prefix and:
		// 1. Copy the remaining bytes of this prefix.
		// 2. Append remaining prefix nodes.
		auto new_prefix = NewInternal(art, child, nullptr, 0, 0, PREFIX);
		new_prefix.data[Count(art)] = prefix.data[Count(art)] - pos - 1;
		memcpy(new_prefix.data, prefix.data + pos + 1, new_prefix.data[Count(art)]);

		if (prefix.ptr->GetType() == PREFIX && !prefix.ptr->IsGate()) {
			new_prefix.Append(art, *prefix.ptr);
		} else {
			*new_prefix.ptr = *prefix.ptr;
		}

	} else if (pos + 1 == prefix.data[Count(art)]) {
		// No prefix bytes after the split.
		child = *prefix.ptr;
	}

	// Set the new count of this node.
	prefix.data[Count(art)] = pos;

	// No bytes left before the split, free this node.
	if (pos == 0) {
		auto freed_gate = node.get().IsGate();
		prefix.ptr->Clear();
		Node::Free(art, node);
		return freed_gate;
	}

	// There are bytes left before the split.
	// The subsequent node replaces the split byte.
	node = *prefix.ptr;
	return false;
}

string Prefix::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {

	// NOTE: we could do this recursively, but the function-call overhead can become kinda crazy
	string str = "";

	reference<const Node> node_ref(node);
	while (node_ref.get().GetType() == PREFIX) {

		Prefix prefix(art, node_ref);
		D_ASSERT(prefix.data[Count(art)] != 0);
		D_ASSERT(prefix.data[Count(art)] <= Count(art));

		str += " Prefix :[";
		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += "] ";

		node_ref = *prefix.ptr;
		if (node_ref.get().IsGate()) {
			break;
		}
	}

	auto subtree = node_ref.get().VerifyAndToString(art, only_verify);
	return only_verify ? "" : str + subtree;
}

void Prefix::Vacuum(ART &art, Node &node, const unordered_set<uint8_t> &indexes) {
	auto prefix_allocator_idx = Node::GetAllocatorIdx(PREFIX);
	bool idx_set = indexes.find(prefix_allocator_idx) != indexes.end();
	auto &allocator = Node::GetAllocator(art, PREFIX);

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == PREFIX) {
		if (idx_set && allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(PREFIX));
		}
		Prefix prefix(art, node_ref, true);
		node_ref = *prefix.ptr;
	}

	node_ref.get().Vacuum(art, indexes);
}

void Prefix::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> node_ref(node);
		while (node_ref.get().GetType() == PREFIX && !node_ref.get().IsGate()) {
			Prefix prefix(art, node_ref, true, true);
			if (!prefix.in_memory) {
				return;
			}
			node_ref = *prefix.ptr;
		}
		return Node::TransformToDeprecated(art, node_ref, allocator);
	}

	// Fast path with memcpy.
	if (art.prefix_count <= ART::DEPRECATED_PREFIX_COUNT) {
		reference<Node> node_ref(node);
		while (node_ref.get().GetType() == PREFIX && !node_ref.get().IsGate()) {
			Prefix prefix(art, node_ref, true, true);
			if (!prefix.in_memory) {
				return;
			}

			Node new_node;
			new_node = allocator->New();
			new_node.SetMetadata(static_cast<uint8_t>(PREFIX));

			Prefix new_prefix(allocator, new_node, ART::DEPRECATED_PREFIX_COUNT);
			new_prefix.data[ART::DEPRECATED_PREFIX_COUNT] = prefix.data[Count(art)];
			memcpy(new_prefix.data, prefix.data, new_prefix.data[ART::DEPRECATED_PREFIX_COUNT]);
			*new_prefix.ptr = *prefix.ptr;

			prefix.ptr->Clear();
			Node::Free(art, node_ref);
			node_ref.get() = new_node;
			node_ref = *new_prefix.ptr;
		}

		return Node::TransformToDeprecated(art, node_ref, allocator);
	}

	// Else, we need to create a new prefix chain.
	Node new_node;
	new_node = allocator->New();
	new_node.SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix new_prefix(allocator, new_node, ART::DEPRECATED_PREFIX_COUNT);

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == PREFIX && !node_ref.get().IsGate()) {
		Prefix prefix(art, node_ref, true, true);
		if (!prefix.in_memory) {
			return;
		}

		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			new_prefix = new_prefix.TransformToDeprecatedAppend(art, allocator, prefix.data[i]);
		}

		*new_prefix.ptr = *prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(node_ref);
		node_ref = *new_prefix.ptr;
	}

	return Node::TransformToDeprecated(art, node_ref, allocator);
}

Prefix Prefix::Append(ART &art, uint8_t byte) {
	if (data[Count(art)] != Count(art)) {
		data[data[Count(art)]] = byte;
		data[Count(art)]++;
		return *this;
	}

	auto prefix = NewInternal(art, *ptr, nullptr, 0, 0, PREFIX);
	return prefix.Append(art, byte);
}

void Prefix::Append(ART &art, Node other) {
	D_ASSERT(other.HasMetadata());

	Prefix prefix = *this;
	while (other.GetType() == PREFIX) {
		if (other.IsGate()) {
			*prefix.ptr = other;
			return;
		}

		Prefix other_prefix(art, other, true);
		for (idx_t i = 0; i < other_prefix.data[Count(art)]; i++) {
			prefix = prefix.Append(art, other_prefix.data[i]);
		}

		*prefix.ptr = *other_prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(other);
		other = *prefix.ptr;
	}
}

Prefix Prefix::NewInternal(ART &art, Node &node, const data_ptr_t data, uint8_t count, idx_t offset, NType type) {
	node = Node::GetAllocator(art, type).New();
	node.SetMetadata(static_cast<uint8_t>(type));

	Prefix prefix(art, node, true);
	prefix.data[Count(art)] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	return prefix;
}

Prefix Prefix::GetTail(ART &art, Node &node) {
	Prefix tail(art, node, true);
	while (tail.ptr->GetType() == PREFIX) {
		tail = Prefix(art, *tail.ptr, true);
	}
	return tail;
}

void Prefix::PrependByte(ART &art, Node &node, uint8_t byte) {
	Prefix prefix(art, node, true);
	memmove(prefix.data + 1, prefix.data, prefix.data[Count(art)]);
	prefix.data[0] = byte;
	prefix.data[Count(art)]++;
}

void Prefix::ConcatGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	Node new_prefix = Node();

	if (!child.HasMetadata()) {
		NewInternal(art, new_prefix, &byte, 1, 0, INLINED);

	} else if (child.GetType() == INLINED) {
		new_prefix = child;
		PrependByte(art, new_prefix, byte);

	} else if (child.GetType() == PREFIX) {
		auto prefix = NewInternal(art, new_prefix, &byte, 1, 0, PREFIX);
		prefix.ptr->Clear();
		prefix.Append(art, child);

	} else {
		auto prefix = NewInternal(art, new_prefix, &byte, 1, 0, PREFIX);
		*prefix.ptr = child;
	}

	new_prefix.SetGate();
	if (parent.GetType() != PREFIX) {
		parent = new_prefix;
		return;
	}

	*GetTail(art, parent).ptr = new_prefix;
}

void Prefix::ConcatChildIsGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	// Create a new prefix and point it to the gate.
	if (parent.GetType() != PREFIX) {
		auto prefix = NewInternal(art, parent, &byte, 1, 0, PREFIX);
		*prefix.ptr = child;
		return;
	}

	auto tail = GetTail(art, parent);
	tail = tail.Append(art, byte);
	*tail.ptr = child;
}

void Prefix::ConcatInlinedPrefix(ART &art, Node &parent, uint8_t byte, const Node &child) {
	bool was_gate = parent.IsGate();
	Node new_prefix = Node();

	if (!child.HasMetadata()) {
		NewInternal(art, new_prefix, &byte, 1, 0, INLINED);

	} else {
		new_prefix = child;
		PrependByte(art, new_prefix, byte);
	}

	if (parent.GetType() == PREFIX) {
		Prefix parent_prefix(art, parent, true);
		Prefix prefix(art, new_prefix, true);

		memmove(prefix.data + parent_prefix.data[Count(art)], prefix.data, prefix.data[Count(art)]);
		memcpy(prefix.data, parent_prefix.data, parent_prefix.data[Count(art)]);
		prefix.data[Count(art)] += parent_prefix.data[Count(art)];

		GetTail(art, parent).ptr->Clear();
		Node::Free(art, parent);
	}

	if (was_gate) {
		new_prefix.SetGate();
	}
	parent = new_prefix;
}

bool Prefix::TraverseInlined(ART &art, Node &l_node, Node &r_node, idx_t &pos) {
	D_ASSERT(r_node.GetType() == INLINED);

	Prefix l_prefix(art, l_node, true);
	Prefix r_prefix(art, r_node, true);

	D_ASSERT(l_prefix.data[Count(art)] == r_prefix.data[Count(art)]);
	for (idx_t i = 0; i < l_prefix.data[Count(art)]; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			pos = i;
			return true;
		}
	}
	throw InternalException("Inlined prefixes cannot match.");
}

void Prefix::ReduceInlinedPrefix(ART &art, Node &node, const idx_t n) {
	Prefix prefix(art, node, true);
	if (n == idx_t(prefix.data[Count(art)] - 1)) {
		Node::Free(art, node);
		return;
	}
	for (idx_t i = 0; i < Count(art) - n - 1; i++) {
		prefix.data[i] = prefix.data[n + i + 1];
	}
}

void Prefix::ReducePrefix(ART &art, Node &node, const idx_t n) {
	Prefix prefix(art, node);
	if (n == idx_t(prefix.data[Count(art)] - 1)) {
		auto next_ptr = *prefix.ptr;
		prefix.ptr->Clear();
		Node::Free(art, node);
		node = next_ptr;
		return;
	}
	for (idx_t i = 0; i < Count(art) - n - 1; i++) {
		prefix.data[i] = prefix.data[n + i + 1];
	}
	prefix.data[Count(art)] -= n + 1;
	prefix.Append(art, *prefix.ptr);
}

bool Prefix::SplitInlined(ART &art, reference<Node> &node, Node &child, uint8_t pos) {
	auto was_gate = node.get().IsGate();
	Prefix prefix(art, node);

	if (pos == 0) {
		// The split is at the first byte.
		// Shift the bytes to the left by one.
		prefix.data[Count(art)]--;
		memmove(prefix.data, prefix.data + 1, prefix.data[Count(art)]);

		if (prefix.data[Count(art)] == 0) {
			// Free the now empty prefix.
			Node::Free(art, node);
		} else {
			// Reset a potential gate, and assign the remaining prefix to the child.
			node.get().ResetGate();
			child = node;
			node.get().Clear();
		}
		return was_gate;
	}

	// Copy all bytes before the split into a new prefix.
	Node new_node = Node();
	auto new_prefix = NewInternal(art, new_node, nullptr, 0, 0, PREFIX);
	memcpy(new_prefix.data, prefix.data, pos);
	new_prefix.data[Count(art)] = pos;
	if (was_gate) {
		new_node.SetGate();
	}

	if (pos + 1 == prefix.data[Count(art)]) {
		// The split is at the last byte.
		// Replace the node with the new prefix.
		Node::Free(art, node);

	} else {
		// Shift all bytes after the split, and turn the node into the child node.
		prefix.data[Count(art)] -= pos + 1;
		memmove(prefix.data, prefix.data + pos + 1, prefix.data[Count(art)]);
		node.get().ResetGate();
		child = node;
	}

	node.get() = new_node;
	node = *new_prefix.ptr;
	return false;
}

Prefix Prefix::TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator, uint8_t byte) {
	if (data[ART::DEPRECATED_PREFIX_COUNT] != ART::DEPRECATED_PREFIX_COUNT) {
		data[data[ART::DEPRECATED_PREFIX_COUNT]] = byte;
		data[ART::DEPRECATED_PREFIX_COUNT]++;
		return *this;
	}

	*ptr = allocator->New();
	ptr->SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix prefix(allocator, *ptr, ART::DEPRECATED_PREFIX_COUNT);
	return prefix.TransformToDeprecatedAppend(art, allocator, byte);
}

} // namespace duckdb
