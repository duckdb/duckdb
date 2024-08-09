#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node7_leaf.hpp"

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
	ptr = type == INLINED ? nullptr : reinterpret_cast<Node *>(data + Count(art) + 1);
	in_memory = true;
}

Prefix::Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count) {
	data = allocator->Get(ptr_p, true);
	ptr = reinterpret_cast<Node *>(data + count + 1);
	in_memory = true;
}

idx_t Prefix::GetMismatchWithOther(const Prefix &l_prefix, const Prefix &r_prefix, const idx_t max_count) {
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t Prefix::GetMismatchWithKey(ART &art, const Node &node, const ARTKey &key, idx_t &depth) {
	Prefix prefix(art, node);
	for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
		if (prefix.data[i] != key[depth]) {
			return i;
		}
		depth++;
	}
	return DConstants::INVALID_INDEX;
}

uint8_t Prefix::GetByte(const ART &art, const Node &node, const uint8_t pos) {
	D_ASSERT(node.IsPrefix());
	Prefix prefix(art, node);
	return prefix.data[pos];
}

Prefix Prefix::NewInternal(ART &art, Node &node, const data_ptr_t data, const uint8_t count, const idx_t offset,
                           const NType type) {
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

void Prefix::NewInlined(ART &art, Node &node, const ARTKey &key, const idx_t depth, const uint8_t count) {
	D_ASSERT(count <= art.prefix_count);
	NewInternal(art, node, key.data, count, depth, INLINED);
}

void Prefix::New(ART &art, reference<Node> &ref, const ARTKey &key, const idx_t depth, idx_t count) {
	idx_t offset = 0;

	while (count) {
		auto min = MinValue(UnsafeNumericCast<idx_t>(Count(art)), count);
		auto this_count = UnsafeNumericCast<uint8_t>(min);
		auto prefix = NewInternal(art, ref, key.data, this_count, offset + depth, PREFIX);

		ref = *prefix.ptr;
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
	auto buffer_count = upper_bounds[Node::GetAllocatorIdx(PREFIX)];

	Node next = node;
	Prefix prefix(art, next, true);

	while (next.GetType() == PREFIX) {
		next = *prefix.ptr;
		if (prefix.ptr->GetType() == PREFIX) {
			prefix.ptr->IncreaseBufferId(buffer_count);
			prefix = Prefix(art, next, true);
		}
	}

	node.IncreaseBufferId(buffer_count);
	prefix.ptr->InitMerge(art, upper_bounds);
}

row_t Prefix::CanInline(ART &art, const Node &parent, const Node &node, const uint8_t byte, const Node &child) {
	if (art.IsUnique()) {
		return INVALID_ROW_ID;
	}
	if (node.GetType() != NType::NODE_7_LEAF && child.GetType() != INLINED) {
		return INVALID_ROW_ID;
	}

	idx_t concat_size = 0;
	uint8_t data[ROW_ID_SIZE];
	if (parent.GetType() == PREFIX) {
		Prefix prefix(art, parent);
		concat_size += prefix.data[art.prefix_count];
		memcpy(data, prefix.data, prefix.data[art.prefix_count]);
	}
	data[concat_size] = byte;
	concat_size++;

	if (node.GetType() == NType::NODE_7_LEAF) {
		if (concat_size < ROW_ID_SIZE) {
			return INVALID_ROW_ID;
		} else {
			return ARTKey(&data[0], ROW_ID_SIZE).GetRowID();
		}
	}

	Prefix prefix(art, child);
	memcpy(data + concat_size, prefix.data, prefix.data[art.prefix_count]);
	concat_size += prefix.data[art.prefix_count];
	if (concat_size < ROW_ID_SIZE) {
		return INVALID_ROW_ID;
	}
	return ARTKey(&data[0], ROW_ID_SIZE).GetRowID();
}

void Prefix::Concat(ART &art, Node &parent, uint8_t byte, const bool is_gate, const Node &child) {
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
		auto pos = Prefix::GetMismatchWithKey(art, node, key, depth);
		if (pos != DConstants::INVALID_INDEX) {
			return pos;
		}

		Prefix prefix(art, node, is_mutable);
		node = *prefix.ptr;
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
		TraverseInlined(art, l_node, r_node, pos);
		return true;
	}

	Prefix l_prefix(art, l_node, true);
	Prefix r_prefix(art, r_node, true);

	idx_t max_count = MinValue(l_prefix.data[Count(art)], r_prefix.data[Count(art)]);
	pos = GetMismatchWithOther(l_prefix, r_prefix, max_count);
	if (pos != DConstants::INVALID_INDEX) {
		return true;
	}

	// Match.
	if (l_prefix.data[Count(art)] == r_prefix.data[Count(art)]) {
		auto r_child = *r_prefix.ptr;
		r_prefix.ptr->Clear();
		Node::Free(art, r_node);
		return l_prefix.ptr->MergeInternal(art, r_child, in_gate);
	}

	pos = max_count;
	if (r_prefix.ptr->GetType() != PREFIX && r_prefix.data[Count(art)] == max_count) {
		// l_prefix contains r_prefix.
		swap(l_node.get(), r_node.get());
		l_node = *r_prefix.ptr;
		return true;
	}
	// r_prefix contains l_prefix.
	l_node = *l_prefix.ptr;
	return true;
}

void Prefix::Reduce(ART &art, Node &node, const idx_t pos) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(pos < Count(art));

	switch (node.GetType()) {
	case INLINED: {
		return ReduceInlinedPrefix(art, node, pos);
	}
	case PREFIX: {
		return ReducePrefix(art, node, pos);
	}
	default:
		throw InternalException("Invalid prefix type for Reduce.");
	}
}

bool Prefix::Split(ART &art, reference<Node> &node, Node &child, const uint8_t pos) {
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
		// Create a new prefix and
		// 1. copy the remaining bytes of this prefix.
		// 2. append remaining prefix nodes.
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

bool Prefix::Insert(ART &art, Node &node, const ARTKey &key, idx_t depth, const ARTKey &row_id, const bool in_gate) {
	reference<Node> next(node);
	auto pos = TraverseMutable(art, next, key, depth);

	// We recurse into the next node, if
	// (1) the prefix matches the key.
	// (2) we reach a gate.
	if (pos == DConstants::INVALID_INDEX) {
		if (next.get().GetType() != NType::PREFIX || next.get().IsGate()) {
			return art.Insert(next, key, depth, row_id, in_gate);
		}
	}

	Node remainder;
	auto byte = GetByte(art, next, UnsafeNumericCast<uint8_t>(pos));
	auto freed_gate = Split(art, next, remainder, UnsafeNumericCast<uint8_t>(pos));
	Node4::New<Node4>(art, next, NType::NODE_4);
	if (freed_gate) {
		next.get().SetGate();
	}

	// Insert the remaining prefix into the new Node4.
	Node4::InsertChild(art, next, byte, remainder);

	if (in_gate) {
		D_ASSERT(pos != ROW_ID_COUNT);
		Node new_prefix;
		auto count = key.len - depth - 1;
		Prefix::NewInlined(art, new_prefix, key, depth + 1, UnsafeNumericCast<uint8_t>(count));
		Node::InsertChild(art, next, key[depth], new_prefix);
		return true;
	}

	Node leaf;
	reference<Node> ref(leaf);
	if (depth + 1 < key.len) {
		// Create the prefix.
		auto count = key.len - depth - 1;
		Prefix::New(art, ref, key, depth + 1, count);
	}
	// Create the inlined leaf.
	Leaf::New(ref, row_id.GetRowID());
	Node4::InsertChild(art, next, key[depth], leaf);
	return true;
}

void Prefix::Fork(ART &art, reference<Node> &node, const idx_t pos, const uint8_t byte, const Node &remainder,
                  const ARTKey &key, const bool freed_gate) {
	if (pos == ROW_ID_COUNT) {
		Node7Leaf::New(art, node);
	} else {
		Node4::New<Node4>(art, node, NType::NODE_4);
	}
	if (freed_gate) {
		node.get().SetGate();
	}

	// Insert the remainder into the new node.
	Node::InsertChild(art, node, byte, remainder);

	// Insert the new key into the new node.
	if (pos == ROW_ID_COUNT) {
		Node::InsertChild(art, node, key[pos]);
		return;
	}

	Node new_prefix;
	auto count = key.len - pos - 1;
	Prefix::NewInlined(art, new_prefix, key, pos + 1, UnsafeNumericCast<uint8_t>(count));
	Node::InsertChild(art, node, key[pos], new_prefix);
}

string Prefix::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {
	string str = "";
	reference<const Node> ref(node);

	Iterator(art, ref, true, false, [&](Prefix &prefix) {
		D_ASSERT(prefix.data[Count(art)] != 0);
		D_ASSERT(prefix.data[Count(art)] <= Count(art));

		str += " Prefix :[ ";
		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += " ] ";
	});

	auto child = ref.get().VerifyAndToString(art, only_verify);
	return only_verify ? "" : str + child;
}

void Prefix::VerifyAllocations(ART &art, const Node &node, unordered_map<uint8_t, idx_t> &node_counts) {
	auto idx = Node::GetAllocatorIdx(PREFIX);
	reference<const Node> ref(node);
	Iterator(art, ref, false, false, [&](Prefix &prefix) { node_counts[idx]++; });
	return ref.get().VerifyAllocations(art, node_counts);
}

void Prefix::Vacuum(ART &art, Node &node, const unordered_set<uint8_t> &indexes) {
	bool set = indexes.find(Node::GetAllocatorIdx(PREFIX)) != indexes.end();
	auto &allocator = Node::GetAllocator(art, PREFIX);

	reference<Node> ref(node);
	while (ref.get().GetType() == PREFIX) {
		if (set && allocator.NeedsVacuum(ref)) {
			ref.get() = allocator.VacuumPointer(ref);
			ref.get().SetMetadata(static_cast<uint8_t>(PREFIX));
		}
		Prefix prefix(art, ref, true);
		ref = *prefix.ptr;
	}

	ref.get().Vacuum(art, indexes);
}

void Prefix::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	// Early-out, if we do not need any transformations.
	if (!allocator) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && !ref.get().IsGate()) {
			Prefix prefix(art, ref, true, true);
			if (!prefix.in_memory) {
				return;
			}
			ref = *prefix.ptr;
		}
		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// Fast path.
	if (art.prefix_count <= DEPRECATED_COUNT) {
		reference<Node> ref(node);
		while (ref.get().GetType() == PREFIX && !ref.get().IsGate()) {
			Prefix prefix(art, ref, true, true);
			if (!prefix.in_memory) {
				return;
			}

			Node new_node;
			new_node = allocator->New();
			new_node.SetMetadata(static_cast<uint8_t>(PREFIX));

			Prefix new_prefix(allocator, new_node, DEPRECATED_COUNT);
			new_prefix.data[DEPRECATED_COUNT] = prefix.data[Count(art)];
			memcpy(new_prefix.data, prefix.data, new_prefix.data[DEPRECATED_COUNT]);
			*new_prefix.ptr = *prefix.ptr;

			prefix.ptr->Clear();
			Node::Free(art, ref);
			ref.get() = new_node;
			ref = *new_prefix.ptr;
		}

		return Node::TransformToDeprecated(art, ref, allocator);
	}

	// Else, we need to create a new prefix chain.
	Node new_node;
	new_node = allocator->New();
	new_node.SetMetadata(static_cast<uint8_t>(PREFIX));
	Prefix new_prefix(allocator, new_node, DEPRECATED_COUNT);

	reference<Node> ref(node);
	while (ref.get().GetType() == PREFIX && !ref.get().IsGate()) {
		Prefix prefix(art, ref, true, true);
		if (!prefix.in_memory) {
			return;
		}

		for (idx_t i = 0; i < prefix.data[Count(art)]; i++) {
			new_prefix = new_prefix.TransformToDeprecatedAppend(art, allocator, prefix.data[i]);
		}

		*new_prefix.ptr = *prefix.ptr;
		Node::GetAllocator(art, PREFIX).Free(ref);
		ref = *new_prefix.ptr;
	}

	return Node::TransformToDeprecated(art, ref, allocator);
}

Prefix Prefix::Append(ART &art, const uint8_t byte) {
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

Prefix Prefix::GetTail(ART &art, const Node &node) {
	Prefix prefix(art, node, true);
	while (prefix.ptr->GetType() == PREFIX) {
		prefix = Prefix(art, *prefix.ptr, true);
	}
	return prefix;
}

void Prefix::PrependByte(ART &art, const Node &node, const uint8_t byte) {
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

void Prefix::TraverseInlined(ART &art, Node &l_node, Node &r_node, idx_t &pos) {
	D_ASSERT(r_node.GetType() == INLINED);
	Prefix l_prefix(art, l_node, true);
	Prefix r_prefix(art, r_node, true);

	D_ASSERT(l_prefix.data[Count(art)] == r_prefix.data[Count(art)]);
	pos = GetMismatchWithOther(l_prefix, r_prefix, l_prefix.data[Count(art)]);
	D_ASSERT(pos != DConstants::INVALID_INDEX);
}

void Prefix::ReduceInlinedPrefix(ART &art, Node &node, const idx_t pos) {
	Prefix prefix(art, node, true);
	if (pos == idx_t(prefix.data[Count(art)] - 1)) {
		Node::Free(art, node);
		return;
	}
	for (idx_t i = 0; i < Count(art) - pos - 1; i++) {
		prefix.data[i] = prefix.data[pos + i + 1];
	}
}

void Prefix::ReducePrefix(ART &art, Node &node, const idx_t pos) {
	Prefix prefix(art, node);
	if (pos == idx_t(prefix.data[Count(art)] - 1)) {
		auto next = *prefix.ptr;
		prefix.ptr->Clear();
		Node::Free(art, node);
		node = next;
		return;
	}
	for (idx_t i = 0; i < Count(art) - pos - 1; i++) {
		prefix.data[i] = prefix.data[pos + i + 1];
	}
	prefix.data[Count(art)] -= pos + 1;
	prefix.Append(art, *prefix.ptr);
}

bool Prefix::SplitInlined(ART &art, reference<Node> &node, Node &child, const uint8_t pos) {
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
