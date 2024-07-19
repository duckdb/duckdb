#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/common/swap.hpp"

namespace duckdb {

Prefix &Prefix::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(NType::PREFIX));

	auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);
	prefix.data[Node::PREFIX_SIZE] = 0;
	return prefix;
}

Prefix &Prefix::New(ART &art, Node &node, uint8_t byte, const Node &next) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetMetadata(static_cast<uint8_t>(NType::PREFIX));

	auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);
	prefix.data[Node::PREFIX_SIZE] = 1;
	prefix.data[0] = byte;
	prefix.ptr = next;
	return prefix;
}

void Prefix::New(ART &art, reference<Node> &node, const ARTKey &key, const uint32_t depth, uint32_t count) {

	if (count == 0) {
		return;
	}
	idx_t copy_count = 0;

	while (count) {
		node.get() = Node::GetAllocator(art, NType::PREFIX).New();
		node.get().SetMetadata(static_cast<uint8_t>(NType::PREFIX));
		auto &prefix = Node::RefMutable<Prefix>(art, node, NType::PREFIX);

		auto this_count = MinValue((uint32_t)Node::PREFIX_SIZE, count);
		prefix.data[Node::PREFIX_SIZE] = (uint8_t)this_count;
		memcpy(prefix.data, key.data + depth + copy_count, this_count);

		node = prefix.ptr;
		copy_count += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {

	Node current_node = node;
	Node next_node;
	while (current_node.HasMetadata() && current_node.GetType() == NType::PREFIX) {
		next_node = Node::RefMutable<Prefix>(art, current_node, NType::PREFIX).ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(current_node);
		current_node = next_node;
	}

	Node::Free(art, current_node);
	node.Clear();
}

void Prefix::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

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

void ConcatenateGate(ART &art, Node &parent_node, const uint8_t byte, Node &child_node) {
	Node new_prefix = Node();

	if (child_node.GetType() == NType::PREFIX) {
		auto &prefix = Prefix::New(art, new_prefix, byte);
		prefix.Append(art, child_node);
	} else {
		Prefix::New(art, new_prefix, byte, child_node);
	}

	new_prefix.SetGate();
	if (parent_node.GetType() != NType::PREFIX) {
		parent_node = new_prefix;
		return;
	}

	// Get the tail.
	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, parent_node, NType::PREFIX);
	while (prefix.get().ptr.GetType() == NType::PREFIX) {
		prefix = Node::RefMutable<Prefix>(art, prefix.get().ptr, NType::PREFIX);
	}
	prefix.get().ptr = new_prefix;
}

void Prefix::Concatenate(ART &art, Node &parent_node, const uint8_t byte, Node &child_node, const bool byte_was_gate) {
	D_ASSERT(parent_node.HasMetadata());
	D_ASSERT(child_node.HasMetadata());

	// Create a new prefix holding the byte.
	if (byte_was_gate) {
		return ConcatenateGate(art, parent_node, byte, child_node);
	}

	// Create a new prefix containing the byte.
	if (parent_node.GetType() != NType::PREFIX && child_node.GetType() != NType::PREFIX) {
		New(art, parent_node, byte, child_node);
		return;
	}

	if (parent_node.GetType() != NType::PREFIX && child_node.GetType() == NType::PREFIX) {
		auto &prefix = New(art, parent_node, byte);
		prefix.Append(art, child_node);
		return;
	}

	// Get the tail.
	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, parent_node, NType::PREFIX);
	while (prefix.get().ptr.GetType() == NType::PREFIX) {
		prefix = Node::RefMutable<Prefix>(art, prefix.get().ptr, NType::PREFIX);
	}

	// Append the byte.
	prefix = prefix.get().Append(art, byte);

	// Append the child.
	if (child_node.GetType() == NType::PREFIX) {
		prefix.get().Append(art, child_node);
		return;
	}

	// Point to the child.
	prefix.get().ptr = child_node;
}

idx_t Prefix::Traverse(ART &art, reference<const Node> &prefix_node, const ARTKey &key, idx_t &depth) {
	D_ASSERT(prefix_node.get().HasMetadata());
	D_ASSERT(prefix_node.get().GetType() == NType::PREFIX);

	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::Ref<const Prefix>(art, prefix_node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().HasMetadata());
		if (prefix_node.get().IsGate()) {
			break;
		}
	}

	return DConstants::INVALID_INDEX;
}

idx_t Prefix::TraverseMutable(ART &art, reference<Node> &prefix_node, const ARTKey &key, idx_t &depth) {
	D_ASSERT(prefix_node.get().HasMetadata());
	D_ASSERT(prefix_node.get().GetType() == NType::PREFIX);

	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().HasMetadata());
		if (prefix_node.get().IsGate()) {
			break;
		}
	}

	return DConstants::INVALID_INDEX;
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position,
                      const bool inside_gate) {
	D_ASSERT(l_node.get().HasMetadata() && l_node.get().GetType() == NType::PREFIX);
	D_ASSERT(r_node.get().HasMetadata() && l_node.get().GetType() == NType::PREFIX);

	auto &l_prefix = Node::RefMutable<Prefix>(art, l_node.get(), NType::PREFIX);
	auto &r_prefix = Node::RefMutable<Prefix>(art, r_node.get(), NType::PREFIX);

	// compare prefix bytes
	idx_t max_count = MinValue(l_prefix.data[Node::PREFIX_SIZE], r_prefix.data[Node::PREFIX_SIZE]);
	for (idx_t i = 0; i < max_count; i++) {
		if (l_prefix.data[i] != r_prefix.data[i]) {
			mismatch_position = i;
			break;
		}
	}

	if (mismatch_position == DConstants::INVALID_INDEX) {

		// prefixes match (so far)
		if (l_prefix.data[Node::PREFIX_SIZE] == r_prefix.data[Node::PREFIX_SIZE]) {
			return l_prefix.ptr.ResolvePrefixes(art, r_prefix.ptr, inside_gate);
		}

		mismatch_position = max_count;

		// l_prefix contains r_prefix
		if (r_prefix.ptr.GetType() != NType::PREFIX && r_prefix.data[Node::PREFIX_SIZE] == max_count) {
			swap(l_node.get(), r_node.get());
			l_node = r_prefix.ptr;

		} else {
			// r_prefix contains l_prefix
			l_node = l_prefix.ptr;
		}
	}

	return true;
}

void Prefix::Reduce(ART &art, Node &prefix_node, const idx_t n) {

	D_ASSERT(prefix_node.HasMetadata());
	D_ASSERT(n < Node::PREFIX_SIZE);

	reference<Prefix> prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);

	// free this prefix node
	if (n == (idx_t)(prefix.get().data[Node::PREFIX_SIZE] - 1)) {
		auto next_ptr = prefix.get().ptr;
		D_ASSERT(next_ptr.HasMetadata());
		prefix.get().ptr.Clear();
		Node::Free(art, prefix_node);
		prefix_node = next_ptr;
		return;
	}

	// shift by n bytes in the current prefix
	for (idx_t i = 0; i < Node::PREFIX_SIZE - n - 1; i++) {
		prefix.get().data[i] = prefix.get().data[n + i + 1];
	}
	D_ASSERT(n < (idx_t)(prefix.get().data[Node::PREFIX_SIZE] - 1));
	prefix.get().data[Node::PREFIX_SIZE] -= n + 1;

	// append the remaining prefix bytes
	prefix.get().Append(art, prefix.get().ptr);
}

bool Prefix::Split(ART &art, reference<Node> &prefix_node, Node &child_node, idx_t position) {
	D_ASSERT(prefix_node.get().HasMetadata());

	auto &prefix = Node::RefMutable<Prefix>(art, prefix_node, NType::PREFIX);

	// The split is at the last prefix byte. Decrease the count and return.
	if (position + 1 == Node::PREFIX_SIZE) {
		prefix.data[Node::PREFIX_SIZE]--;
		prefix_node = prefix.ptr;
		child_node = prefix.ptr;
		return false;
	}

	// Create a new child prefix node and append:
	// 1. The remaining bytes of this prefix.
	// 2. The remaining prefix nodes.
	if (position + 1 < prefix.data[Node::PREFIX_SIZE]) {
		reference<Prefix> child_prefix = New(art, child_node);
		for (idx_t i = position + 1; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			child_prefix = child_prefix.get().Append(art, prefix.data[i]);
		}

		D_ASSERT(prefix.ptr.HasMetadata());
		if (prefix.ptr.GetType() == NType::PREFIX) {
			child_prefix.get().Append(art, prefix.ptr);
		} else {
			child_prefix.get().ptr = prefix.ptr;
		}

	} else if (position + 1 == prefix.data[Node::PREFIX_SIZE]) {
		// No prefix bytes after the split.
		child_node = prefix.ptr;
	}

	// Set the new count of this node.
	prefix.data[Node::PREFIX_SIZE] = UnsafeNumericCast<uint8_t>(position);

	// No bytes left before the split, free this node.
	if (position == 0) {
		auto freed_gate = prefix_node.get().IsGate();
		prefix.ptr.Clear();
		Node::Free(art, prefix_node.get());
		return freed_gate;
	}

	// There are bytes left before the split. The subsequent node replaces the split byte.
	prefix_node = prefix.ptr;
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

Prefix &Prefix::Append(ART &art, const uint8_t byte) {

	reference<Prefix> prefix(*this);

	// we need a new prefix node
	if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
		prefix = New(art, prefix.get().ptr);
	}

	prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
	prefix.get().data[Node::PREFIX_SIZE]++;
	return prefix.get();
}

void Prefix::Append(ART &art, Node other_prefix) {
	D_ASSERT(other_prefix.HasMetadata());

	reference<Prefix> prefix(*this);
	while (other_prefix.GetType() == NType::PREFIX && !other_prefix.IsGate()) {

		// copy prefix bytes
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
