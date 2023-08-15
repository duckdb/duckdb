#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/metadata/metadata_reader.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/common/swap.hpp"

namespace duckdb {

Prefix &Prefix::New(ART &art, Node &node) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetType((uint8_t)NType::PREFIX);

	auto &prefix = Prefix::Get(art, node);
	prefix.data[Node::PREFIX_SIZE] = 0;
	return prefix;
}

Prefix &Prefix::New(ART &art, Node &node, uint8_t byte, Node next) {

	node = Node::GetAllocator(art, NType::PREFIX).New();
	node.SetType((uint8_t)NType::PREFIX);

	auto &prefix = Prefix::Get(art, node);
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
		node.get().SetType((uint8_t)NType::PREFIX);
		auto &prefix = Prefix::Get(art, node);

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
	while (current_node.IsSet() && !current_node.IsSerialized() && current_node.GetType() == NType::PREFIX) {
		next_node = Prefix::Get(art, current_node).ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(current_node);
		current_node = next_node;
	}

	Node::Free(art, current_node);
	node.Reset();
}

void Prefix::InitializeMerge(ART &art, Node &node, const ARTFlags &flags) {

	auto merge_buffer_count = flags.merge_buffer_counts[(uint8_t)NType::PREFIX - 1];

	Node next_node = node;
	reference<Prefix> prefix = Prefix::Get(art, next_node);

	while (next_node.GetType() == NType::PREFIX) {
		next_node = prefix.get().ptr;
		if (prefix.get().ptr.GetType() == NType::PREFIX) {
			prefix.get().ptr.AddToBufferID(merge_buffer_count);
			prefix = Prefix::Get(art, next_node);
		}
	}

	node.AddToBufferID(merge_buffer_count);
	prefix.get().ptr.InitializeMerge(art, flags);
}

void Prefix::Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node) {

	D_ASSERT(prefix_node.IsSet() && !prefix_node.IsSerialized());
	D_ASSERT(child_prefix_node.IsSet());

	if (child_prefix_node.IsSerialized()) {
		child_prefix_node.Deserialize(art);
	}

	// append a byte and a child_prefix to prefix
	if (prefix_node.GetType() == NType::PREFIX) {

		// get the tail
		reference<Prefix> prefix = Prefix::Get(art, prefix_node);
		D_ASSERT(prefix.get().ptr.IsSet() && !prefix.get().ptr.IsSerialized());

		while (prefix.get().ptr.GetType() == NType::PREFIX) {
			prefix = Prefix::Get(art, prefix.get().ptr);
			D_ASSERT(prefix.get().ptr.IsSet() && !prefix.get().ptr.IsSerialized());
		}

		// append the byte
		prefix = prefix.get().Append(art, byte);

		if (child_prefix_node.GetType() == NType::PREFIX) {
			// append the child prefix
			prefix.get().Append(art, child_prefix_node);
		} else {
			// set child_prefix_node to succeed prefix
			prefix.get().ptr = child_prefix_node;
		}
		return;
	}

	// create a new prefix node containing the byte, then append the child_prefix to it
	if (prefix_node.GetType() != NType::PREFIX && child_prefix_node.GetType() == NType::PREFIX) {

		auto child_prefix = child_prefix_node;
		auto &prefix = Prefix::New(art, prefix_node, byte, Node());
		prefix.Append(art, child_prefix);
		return;
	}

	// neither prefix nor child_prefix are prefix nodes
	// create a new prefix containing the byte
	Prefix::New(art, prefix_node, byte, child_prefix_node);
}

idx_t Prefix::Traverse(ART &art, reference<Node> &prefix_node, const ARTKey &key, idx_t &depth) {

	D_ASSERT(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());
	D_ASSERT(prefix_node.get().GetType() == NType::PREFIX);

	// compare prefix nodes to key bytes
	while (prefix_node.get().GetType() == NType::PREFIX) {
		auto &prefix = Prefix::Get(art, prefix_node);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().IsSet());
		if (prefix_node.get().IsSerialized()) {
			prefix_node.get().Deserialize(art);
		}
	}

	return DConstants::INVALID_INDEX;
}

bool Prefix::Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position) {

	auto &l_prefix = Prefix::Get(art, l_node.get());
	auto &r_prefix = Prefix::Get(art, r_node.get());

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
			return l_prefix.ptr.ResolvePrefixes(art, r_prefix.ptr);
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

	D_ASSERT(prefix_node.IsSet() && !prefix_node.IsSerialized());
	D_ASSERT(n < Node::PREFIX_SIZE);

	reference<Prefix> prefix = Prefix::Get(art, prefix_node);

	// free this prefix node
	if (n == (idx_t)(prefix.get().data[Node::PREFIX_SIZE] - 1)) {
		auto next_ptr = prefix.get().ptr;
		D_ASSERT(next_ptr.IsSet());
		prefix.get().ptr.Reset();
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

void Prefix::Split(ART &art, reference<Node> &prefix_node, Node &child_node, idx_t position) {

	D_ASSERT(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());

	auto &prefix = Prefix::Get(art, prefix_node);

	// the split is at the last byte of this prefix, so the child_node contains all subsequent
	// prefix nodes (prefix.ptr) (if any), and the count of this prefix decreases by one,
	// then, we reference prefix.ptr, to overwrite it with a new node later
	if (position + 1 == Node::PREFIX_SIZE) {
		prefix.data[Node::PREFIX_SIZE]--;
		prefix_node = prefix.ptr;
		child_node = prefix.ptr;
		return;
	}

	// append the remaining bytes after the split
	if (position + 1 < prefix.data[Node::PREFIX_SIZE]) {
		reference<Prefix> child_prefix = Prefix::New(art, child_node);
		for (idx_t i = position + 1; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			child_prefix = child_prefix.get().Append(art, prefix.data[i]);
		}

		D_ASSERT(prefix.ptr.IsSet());
		if (prefix.ptr.IsSerialized()) {
			prefix.ptr.Deserialize(art);
		}

		if (prefix.ptr.GetType() == NType::PREFIX) {
			child_prefix.get().Append(art, prefix.ptr);
		} else {
			// this is the last prefix node of the prefix
			child_prefix.get().ptr = prefix.ptr;
		}
	}

	// this is the last prefix node of the prefix
	if (position + 1 == prefix.data[Node::PREFIX_SIZE]) {
		child_node = prefix.ptr;
	}

	// set the new size of this node
	prefix.data[Node::PREFIX_SIZE] = position;

	// no bytes left before the split, free this node
	if (position == 0) {
		prefix.ptr.Reset();
		Node::Free(art, prefix_node.get());
		return;
	}

	// bytes left before the split, reference subsequent node
	prefix_node = prefix.ptr;
	return;
}

string Prefix::VerifyAndToString(ART &art, Node &node, const bool only_verify) {

	// NOTE: we could do this recursively, but the function-call overhead can become kinda crazy
	string str = "";

	reference<Node> node_ref(node);
	while (node_ref.get().GetType() == NType::PREFIX) {

		auto &prefix = Prefix::Get(art, node_ref);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] != 0);
		D_ASSERT(prefix.data[Node::PREFIX_SIZE] <= Node::PREFIX_SIZE);

		str += " prefix_bytes:[";
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			str += to_string(prefix.data[i]) + "-";
		}
		str += "] ";

		if (prefix.ptr.IsSerialized()) {
			return str + " serialized";
		}
		node_ref = prefix.ptr;
	}

	return str + node_ref.get().VerifyAndToString(art, only_verify);
}

BlockPointer Prefix::Serialize(ART &art, Node &node, MetadataWriter &writer) {
	reference<Node> first_non_prefix(node);
	idx_t total_count = Prefix::TotalCount(art, first_non_prefix);
	auto child_block_pointer = first_non_prefix.get().Serialize(art, writer);

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::PREFIX);
	writer.Write<idx_t>(total_count);

	reference<Node> current_node(node);
	while (current_node.get().GetType() == NType::PREFIX) {

		// write prefix bytes
		D_ASSERT(!current_node.get().IsSerialized());
		auto &prefix = Prefix::Get(art, current_node);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			writer.Write(prefix.data[i]);
		}

		current_node = prefix.ptr;
	}

	// write child block pointer
	writer.Write(child_block_pointer.block_id);
	writer.Write(child_block_pointer.offset);

	return block_pointer;
}

void Prefix::Deserialize(ART &art, Node &node, MetadataReader &reader) {
	auto total_count = reader.Read<idx_t>();
	reference<Node> current_node(node);

	while (total_count) {
		current_node.get() = Node::GetAllocator(art, NType::PREFIX).New();
		current_node.get().SetType((uint8_t)NType::PREFIX);

		auto &prefix = Prefix::Get(art, current_node);
		prefix.data[Node::PREFIX_SIZE] = MinValue((idx_t)Node::PREFIX_SIZE, total_count);

		// read bytes
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			prefix.data[i] = reader.Read<uint8_t>();
		}

		total_count -= prefix.data[Node::PREFIX_SIZE];
		current_node = prefix.ptr;
		prefix.ptr.Reset();
	}

	// read child block pointer
	current_node.get() = Node(reader);
}

void Prefix::Vacuum(ART &art, Node &node, const ARTFlags &flags) {

	bool flag_set = flags.vacuum_flags[(uint8_t)NType::PREFIX - 1];
	auto &allocator = Node::GetAllocator(art, NType::PREFIX);

	reference<Node> node_ref(node);
	while (!node_ref.get().IsSerialized() && node_ref.get().GetType() == NType::PREFIX) {
		if (flag_set && allocator.NeedsVacuum(node_ref)) {
			node_ref.get() = allocator.VacuumPointer(node_ref);
			node_ref.get().SetType((uint8_t)NType::PREFIX);
		}
		auto &prefix = Prefix::Get(art, node_ref);
		node_ref = prefix.ptr;
	}

	node_ref.get().Vacuum(art, flags);
}

Prefix &Prefix::Append(ART &art, const uint8_t byte) {

	reference<Prefix> prefix(*this);

	// we need a new prefix node
	if (prefix.get().data[Node::PREFIX_SIZE] == Node::PREFIX_SIZE) {
		prefix = Prefix::New(art, prefix.get().ptr);
	}

	prefix.get().data[prefix.get().data[Node::PREFIX_SIZE]] = byte;
	prefix.get().data[Node::PREFIX_SIZE]++;
	return prefix.get();
}

void Prefix::Append(ART &art, Node other_prefix) {

	// NOTE: all usages of this function already deserialize the other prefix
	D_ASSERT(other_prefix.IsSet() && !other_prefix.IsSerialized());

	reference<Prefix> prefix(*this);
	while (other_prefix.GetType() == NType::PREFIX) {

		// copy prefix bytes
		auto &other = Prefix::Get(art, other_prefix);
		for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
			prefix = prefix.get().Append(art, other.data[i]);
		}

		D_ASSERT(other.ptr.IsSet());
		if (other.ptr.IsSerialized()) {
			other.ptr.Deserialize(art);
		}

		prefix.get().ptr = other.ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(other_prefix);
		other_prefix = prefix.get().ptr;
	}

	D_ASSERT(prefix.get().ptr.GetType() != NType::PREFIX);
}

idx_t Prefix::TotalCount(ART &art, reference<Node> &node) {

	// NOTE: first prefix in the prefix chain is already deserialized
	D_ASSERT(node.get().IsSet() && !node.get().IsSerialized());

	idx_t count = 0;
	while (node.get().GetType() == NType::PREFIX) {
		auto &prefix = Prefix::Get(art, node);
		count += prefix.data[Node::PREFIX_SIZE];

		if (prefix.ptr.IsSerialized()) {
			prefix.ptr.Deserialize(art);
		}
		node = prefix.ptr;
	}
	return count;
}

} // namespace duckdb
