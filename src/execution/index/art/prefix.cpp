#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/swap.hpp"

namespace duckdb {

template <class PREFIX>
PREFIX &NewPrefix(ART &art, Node &node, const data_ptr_t data, uint8_t count, idx_t offset, NType type) {
	node = Node::GetAllocator(art, type).New();
	node.SetMetadata(static_cast<uint8_t>(type));

	auto &prefix = Node::RefMutable<PREFIX>(art, node, type);
	prefix.data[Node::PREFIX_SIZE] = count;
	if (data) {
		D_ASSERT(count);
		memcpy(prefix.data, data + offset, count);
	}
	return prefix;
}

//===--------------------------------------------------------------------===//
// PrefixInlined
//===--------------------------------------------------------------------===//

void PrefixInlined::New(ART &art, Node &node, const ARTKey &key, uint32_t depth, uint8_t count) {
	NewPrefix<PrefixInlined>(art, node, key.data, count, depth, NType::PREFIX_INLINED);
}

//===--------------------------------------------------------------------===//
// Prefix
//===--------------------------------------------------------------------===//

Prefix &Prefix::New(ART &art, Node &node) {
	return NewPrefix<Prefix>(art, node, nullptr, 0, 0, NType::PREFIX);
}

void Prefix::New(ART &art, reference<Node> &node, const ARTKey &key, uint32_t depth, uint32_t count) {
	idx_t offset = 0;
	while (count) {
		auto this_count = uint8_t(MinValue(uint32_t(Node::PREFIX_SIZE), count));
		auto &prefix = NewPrefix<Prefix>(art, node, key.data, this_count, offset + depth, NType::PREFIX);

		node = prefix.ptr;
		offset += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {
	Node next;
	while (node.HasMetadata() && node.IsPrefix()) {
		D_ASSERT(node.GetType() != NType::PREFIX_INLINED);
		next = Node::RefMutable<Prefix>(art, node, NType::PREFIX).ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(node);
		node = next;
	}

	Node::Free(art, node);
	node.Clear();
}

void Prefix::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {
	D_ASSERT(node.GetType() != NType::PREFIX_INLINED);
	auto merge_buffer_count = flags.merge_buffer_counts[static_cast<uint8_t>(NType::PREFIX) - 1];

	Node next_node = node;
	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, next_node, NType::PREFIX);

	while (next_node.GetType() == NType::PREFIX) {
		next_node = prefix.get().ptr;
		if (prefix.get().ptr.GetType() == NType::PREFIX) {
			prefix.get().ptr.IncreaseBufferId(merge_buffer_count);
			prefix = Node::RefMutable<Prefix>(art, next_node, NType::PREFIX);
		}
	}

	node.IncreaseBufferId(merge_buffer_count);
	prefix.get().ptr.InitializeMerge(art, flags);
}

Prefix &GetTail(ART &art, Node &node) {
	reference<Prefix> tail = Node::RefMutable<Prefix>(art, node, NType::PREFIX);
	while (tail.get().ptr.GetType() == NType::PREFIX) {
		tail = Node::RefMutable<Prefix>(art, tail.get().ptr, NType::PREFIX);
	}
	return tail;
}

void PrependByte(ART &art, Node &node, uint8_t byte) {
	auto &prefix = Node::RefMutable<PrefixInlined>(art, node, NType::PREFIX_INLINED);
	memmove(prefix.data + 1, prefix.data, prefix.data[Node::PREFIX_SIZE]);
	prefix.data[0] = byte;
	prefix.data[Node::PREFIX_SIZE]++;
}

void ConcatGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	Node new_prefix = Node();

	if (!child.HasMetadata()) {
		NewPrefix<PrefixInlined>(art, new_prefix, &byte, 1, 0, NType::PREFIX_INLINED);

	} else if (child.GetType() == NType::PREFIX_INLINED) {
		new_prefix = child;
		PrependByte(art, new_prefix, byte);

	} else if (child.GetType() == NType::PREFIX) {
		auto &prefix = NewPrefix<Prefix>(art, new_prefix, &byte, 1, 0, NType::PREFIX);
		prefix.ptr.Clear();
		prefix.Append(art, child);

	} else {
		auto &prefix = NewPrefix<Prefix>(art, new_prefix, &byte, 1, 0, NType::PREFIX);
		prefix.ptr = child;
	}

	new_prefix.SetGate();
	if (parent.GetType() != NType::PREFIX) {
		parent = new_prefix;
		return;
	}
	GetTail(art, parent).ptr = new_prefix;
}

void ConcatChildIsGate(ART &art, Node &parent, uint8_t byte, const Node &child) {
	// Create a new prefix and point it to the gate.
	if (parent.GetType() != NType::PREFIX) {
		auto &prefix = NewPrefix<Prefix>(art, parent, &byte, 1, 0, NType::PREFIX);
		prefix.ptr = child;
		return;
	}

	reference<Prefix> tail = GetTail(art, parent);
	tail = tail.get().Append(art, byte);
	tail.get().ptr = child;
}

void ConcatInlinedPrefix(ART &art, Node &parent, uint8_t byte, const Node &child) {
	bool was_gate = parent.IsGate();
	Node new_prefix = Node();

	if (!child.HasMetadata()) {
		NewPrefix<PrefixInlined>(art, new_prefix, &byte, 1, 0, NType::PREFIX_INLINED);

	} else {
		new_prefix = child;
		PrependByte(art, new_prefix, byte);
	}

	if (parent.GetType() == NType::PREFIX) {
		auto &parent_prefix = Node::RefMutable<Prefix>(art, parent, NType::PREFIX);
		auto &prefix = Node::RefMutable<PrefixInlined>(art, new_prefix, NType::PREFIX_INLINED);

		memmove(prefix.data + parent_prefix.data[Node::PREFIX_SIZE], prefix.data, prefix.data[Node::PREFIX_SIZE]);
		memcpy(prefix.data, parent_prefix.data, parent_prefix.data[Node::PREFIX_SIZE]);
		prefix.data[Node::PREFIX_SIZE] += parent_prefix.data[Node::PREFIX_SIZE];

		GetTail(art, parent).ptr.Clear();
		Node::Free(art, parent);
	}

	if (was_gate) {
		new_prefix.SetGate();
	}
	parent = new_prefix;
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

	if (!child.HasMetadata() || child.GetType() == NType::PREFIX_INLINED) {
		return ConcatInlinedPrefix(art, parent, byte, child);
	}

	if (parent.GetType() != NType::PREFIX) {
		auto &prefix = NewPrefix<Prefix>(art, parent, &byte, 1, 0, NType::PREFIX);
		if (child.GetType() == NType::PREFIX) {
			prefix.Append(art, child);
		} else {
			prefix.ptr = child;
		}
		return;
	}

	reference<Prefix> tail = GetTail(art, parent);
	tail = tail.get().Append(art, byte);

	if (child.GetType() == NType::PREFIX) {
		tail.get().Append(art, child);
	} else {
		tail.get().ptr = child;
	}
}

template <class PREFIX, class NODE>
idx_t Prefix::Traverse(ART &art, reference<NODE> &node, const ARTKey &key, idx_t &depth,
                       PREFIX &(*func)(const ART &art, const Node ptr, const NType type)) {

	D_ASSERT(node.get().HasMetadata());
	D_ASSERT(node.get().GetType() == NType::PREFIX);

	while (node.get().GetType() == NType::PREFIX) {
		auto &prefix = func(art, node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		node = prefix.ptr;
		D_ASSERT(node.get().HasMetadata());
		if (node.get().IsGate()) {
			break;
		}
	}

	return DConstants::INVALID_INDEX;
}

bool TraverseInlined(ART &art, Node &l, Node &r, idx_t &mismatch_pos) {
	D_ASSERT(r.GetType() == NType::PREFIX_INLINED);

	auto &l_prefix = Node::RefMutable<PrefixInlined>(art, l, NType::PREFIX_INLINED);
	auto &r_prefix = Node::RefMutable<PrefixInlined>(art, r, NType::PREFIX_INLINED);

	D_ASSERT(l_prefix.data[Node::PREFIX_SIZE] == r_prefix.data[Node::PREFIX_SIZE]);
	for (idx_t i = 0; i < l_prefix.data[Node::PREFIX_SIZE]; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			mismatch_pos = i;
			return true;
		}
	}
	throw InternalException("Inlined prefixes cannot match.");
}

bool Prefix::Traverse(ART &art, reference<Node> &l, reference<Node> &r, idx_t &mismatch_pos, bool inside_gate) {
	D_ASSERT(l.get().HasMetadata());
	D_ASSERT(r.get().HasMetadata());

	if (l.get().GetType() == NType::PREFIX_INLINED) {
		return TraverseInlined(art, l, r, mismatch_pos);
	}

	auto &l_prefix = Node::RefMutable<Prefix>(art, l.get(), NType::PREFIX);
	auto &r_prefix = Node::RefMutable<Prefix>(art, r.get(), NType::PREFIX);

	idx_t max_count = MinValue(l_prefix.data[Node::PREFIX_SIZE], r_prefix.data[Node::PREFIX_SIZE]);
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			mismatch_pos = i;
			break;
		}
	}
	if (mismatch_pos != DConstants::INVALID_INDEX) {
		return true;
	}

	// Match.
	if (l_prefix.data[Node::PREFIX_SIZE] == r_prefix.data[Node::PREFIX_SIZE]) {
		return l_prefix.ptr.MergeInternal(art, r_prefix.ptr, inside_gate);
	}

	mismatch_pos = max_count;
	if (r_prefix.ptr.GetType() != NType::PREFIX && r_prefix.data[Node::PREFIX_SIZE] == max_count) {
		// l_prefix contains r_prefix
		swap(l.get(), r.get());
		l = r_prefix.ptr;
	} else {
		// r_prefix contains l_prefix
		l = l_prefix.ptr;
	}
	return true;
}

uint8_t Prefix::GetByte(const ART &art, const Node &node, const idx_t pos) {
	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX_INLINED: {
		auto &prefix = Node::Ref<const PrefixInlined>(art, node, type);
		return prefix.data[pos];
	}
	case NType::PREFIX: {
		auto &prefix = Node::Ref<const Prefix>(art, node, type);
		return prefix.data[pos];
	}
	default:
		throw InternalException("Invalid prefix type for GetByte.");
	}
}

void ReduceInlinedPrefix(ART &art, Node &node, const idx_t n, const NType type) {
	auto &prefix = Node::RefMutable<PrefixInlined>(art, node, type);
	if (n == idx_t(prefix.data[Node::PREFIX_SIZE] - 1)) {
		Node::Free(art, node);
		return;
	}
	for (idx_t i = 0; i < Node::PREFIX_SIZE - n - 1; i++) {
		prefix.data[i] = prefix.data[n + i + 1];
	}
}

void ReducePrefix(ART &art, Node &node, const idx_t n, const NType type) {
	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, node, type);
	if (n == idx_t(prefix.get().data[Node::PREFIX_SIZE] - 1)) {
		auto next_ptr = prefix.get().ptr;
		prefix.get().ptr.Clear();
		Node::Free(art, node);
		node = next_ptr;
		return;
	}
	for (idx_t i = 0; i < Node::PREFIX_SIZE - n - 1; i++) {
		prefix.get().data[i] = prefix.get().data[n + i + 1];
	}
	prefix.get().data[Node::PREFIX_SIZE] -= n + 1;
	prefix.get().Append(art, prefix.get().ptr);
}

void Prefix::Reduce(ART &art, Node &node, const idx_t n) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(n < Node::PREFIX_SIZE);

	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX_INLINED: {
		return ReduceInlinedPrefix(art, node, n, type);
	}
	case NType::PREFIX: {
		return ReducePrefix(art, node, n, type);
	}
	default:
		throw InternalException("Invalid prefix type for Reduce.");
	}
}

// TODO
bool Prefix::Split(ART &art, Node &node, Node &child, idx_t position) {
	D_ASSERT(node.HasMetadata());
	D_ASSERT(node.GetType() == NType::PREFIX);

	auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);

	// The split is at the last prefix byte. Decrease the count and return.
	if (position + 1 == Node::PREFIX_SIZE) {
		prefix.data[Node::PREFIX_SIZE]--;
		node = prefix.ptr;
		child = prefix.ptr;
		return false;
	}

	// Create a new child prefix node and append:
	// 1. The remaining bytes of this prefix.
	// 2. The remaining prefix nodes.
	if (position + 1 < prefix.data[Node::PREFIX_SIZE]) {
		reference<Prefix> child_prefix = New(art, child);
		for (idx_t i = position + 1; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			child_prefix = child_prefix.get().Append(art, prefix.data[i]);
		}

		D_ASSERT(prefix.ptr.HasMetadata());
		if (prefix.ptr.GetType() == NType::PREFIX && !prefix.ptr.IsGate()) {
			child_prefix.get().Append(art, prefix.ptr);
		} else {
			child_prefix.get().ptr = prefix.ptr;
		}

	} else if (position + 1 == prefix.data[Node::PREFIX_SIZE]) {
		// No prefix bytes after the split.
		child = prefix.ptr;
	}

	// Set the new count of this node.
	prefix.data[Node::PREFIX_SIZE] = UnsafeNumericCast<uint8_t>(position);

	// No bytes left before the split, free this node.
	if (position == 0) {
		auto freed_gate = node.IsGate();
		prefix.ptr.Clear();
		Node::Free(art, node);
		return freed_gate;
	}

	// There are bytes left before the split. The subsequent node replaces the split byte.
	node = prefix.ptr;
	return false;
}

string Prefix::VerifyAndToString(ART &art, const Node &node, const bool only_verify) {

	// NOTE: we could do this recursively, but the function-call overhead can become kinda crazy
	string str = "";

	reference<const Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {

		auto &prefix = Node::Ref<const Prefix>(art, node_ref, NType::PREFIX);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] != 0);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] <= Node::PREFIX_SIZE);

		str += " prefix_bytes:[";
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += "] ";

		node_ref = prefix.ptr;
		if (node_ref.get().IsGate()) {
			break;
		}
	}

	auto subtree = node_ref.get().VerifyAndToString(art, only_verify);
	return only_verify ? "" : str + subtree;
}

void Prefix::Vacuum(ART &art, Node &node, const ARTFlags &flags) {

	bool flag_set = flags.vacuum_flags[static_cast<uint8_t>(NType::PREFIX) - 1];
	auto &allocator = Node::GetAllocator(art, NType::PREFIX);

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {
		if (flag_set && allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetMetadata(static_cast<uint8_t>(NType::PREFIX));
		}
		auto &prefix = Node::RefMutable<Prefix>(art, node_ref, NType::PREFIX);
		node_ref = prefix.ptr;
	}

	node_ref.get().Vacuum(art, flags);
}

void Prefix::TransformToDeprecated(ART &art, Node &node) {

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {
		auto prefix_ptr = Node::GetInMemoryPtr<Prefix>(art, node_ref, NType::PREFIX);
		if (!prefix_ptr) {
			return;
		}
		node_ref = prefix_ptr->ptr;
	}

	Node::TransformToDeprecated(art, node_ref.get());
}

// TODO
Prefix &Prefix::Append(ART &art, const uint8_t byte) {

	reference<Prefix> prefix(*this);

	// The current prefix is full. Thus, we append a new prefix node.
	if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
		prefix = New(art, prefix.get().ptr);
	}

	prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
	prefix.get().data[Node::PREFIX_SIZE]++;
	return prefix.get();
}

// TODO
void Prefix::Append(ART &art, Node other_prefix) {
	D_ASSERT(other_prefix.HasMetadata());

	reference<Prefix> prefix(*this);
	while (other_prefix.GetType() == NType::PREFIX) {

		if (other_prefix.IsGate()) {
			prefix.get().ptr = other_prefix;
			return;
		}

		// Copy all prefix bytes of other_prefix into this prefix.
		auto &other = Node::RefMutable<Prefix>(art, other_prefix, NType::PREFIX);
		for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
			prefix = prefix.get().Append(art, other.data[i]);
		}
		D_ASSERT(other.ptr.HasMetadata());

		prefix.get().ptr = other.ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(other_prefix);
		other_prefix = prefix.get().ptr;
	}

	D_ASSERT(prefix.get().ptr.GetType() != NType::PREFIX || prefix.get().ptr.IsGate());
}

} // namespace duckdb
