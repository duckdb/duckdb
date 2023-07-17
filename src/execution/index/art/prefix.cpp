#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"
#include "duckdb/common/swap.hpp"

namespace duckdb {

Prefix &Prefix::New(ART &art, Node &node) {

	node.SetPtr(Node::GetAllocator(art, NType::PREFIX).New());
	node.type = (uint8_t)NType::PREFIX;

	auto &prefix = Prefix::Get(art, node);
	prefix.data[Node::PREFIX_SIZE] = 0;
	return prefix;
}

Prefix &Prefix::New(ART &art, Node &node, uint8_t byte, Node next) {

	node.SetPtr(Node::GetAllocator(art, NType::PREFIX).New());
	node.type = (uint8_t)NType::PREFIX;

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
		node.get().SetPtr(Node::GetAllocator(art, NType::PREFIX).New());
		node.get().type = (uint8_t)NType::PREFIX;
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

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSwizzled());

	auto &child = Prefix::Get(art, node).ptr;
	Node::Free(art, child);
}

void Prefix::Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node) {

	D_ASSERT(prefix_node.IsSet() && !prefix_node.IsSwizzled());
	D_ASSERT(child_prefix_node.IsSet());

	if (child_prefix_node.IsSwizzled()) {
		child_prefix_node.Deserialize(art);
	}

	// append a byte and a child_prefix to prefix
	if (prefix_node.DecodeARTNodeType() == NType::PREFIX) {

		// get the tail
		reference<Prefix> prefix = Prefix::Get(art, prefix_node);
		D_ASSERT(prefix.get().ptr.IsSet() && !prefix.get().ptr.IsSwizzled());

		while (prefix.get().ptr.DecodeARTNodeType() == NType::PREFIX) {
			prefix = Prefix::Get(art, prefix.get().ptr);
			D_ASSERT(prefix.get().ptr.IsSet() && !prefix.get().ptr.IsSwizzled());
		}

		// append the byte
		prefix = prefix.get().Append(art, byte);

		if (child_prefix_node.DecodeARTNodeType() == NType::PREFIX) {
			// append the child prefix
			prefix.get().Append(art, child_prefix_node);
		} else {
			// set child_prefix_node to succeed prefix
			prefix.get().ptr = child_prefix_node;
		}
		return;
	}

	// create a new prefix node containing the byte, then append the child_prefix to it
	if (prefix_node.DecodeARTNodeType() != NType::PREFIX && child_prefix_node.DecodeARTNodeType() == NType::PREFIX) {

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

	D_ASSERT(prefix_node.get().IsSet() && !prefix_node.get().IsSwizzled());
	D_ASSERT(prefix_node.get().DecodeARTNodeType() == NType::PREFIX);

	// compare prefix nodes to key bytes
	while (prefix_node.get().DecodeARTNodeType() == NType::PREFIX) {
		auto &prefix = Prefix::Get(art, prefix_node);
		for (idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
			if (prefix.data[i] != key[depth]) {
				return i;
			}
			depth++;
		}
		prefix_node = prefix.ptr;
		D_ASSERT(prefix_node.get().IsSet());
		if (prefix_node.get().IsSwizzled()) {
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
		if (r_prefix.ptr.DecodeARTNodeType() != NType::PREFIX && r_prefix.data[Node::PREFIX_SIZE] == max_count) {
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

	D_ASSERT(prefix_node.IsSet() && !prefix_node.IsSwizzled());
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

	D_ASSERT(prefix_node.get().IsSet() && !prefix_node.get().IsSwizzled());

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
		if (prefix.ptr.IsSwizzled()) {
			prefix.ptr.Deserialize(art);
		}

		if (prefix.ptr.DecodeARTNodeType() == NType::PREFIX) {
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

string Prefix::VerifyAndToString(ART &art, const bool only_verify) {

	D_ASSERT(data[Node::PREFIX_SIZE] != 0);
	D_ASSERT(data[Node::PREFIX_SIZE] <= Node::PREFIX_SIZE);

	string str = " prefix_bytes:[";
	for (idx_t i = 0; i < data[Node::PREFIX_SIZE]; i++) {
		str += to_string(data[i]) + "-";
	}
	str += "] ";

	str = only_verify ? ptr.VerifyAndToString(art, only_verify) : str + ptr.VerifyAndToString(art, only_verify);
	return str;
}

BlockPointer Prefix::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into the child and retrieve its block pointer
	auto child_block_pointer = ptr.Serialize(art, writer);

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::PREFIX);
	writer.Write<uint8_t>(data[Node::PREFIX_SIZE]);

	// write prefix bytes
	for (idx_t i = 0; i < data[Node::PREFIX_SIZE]; i++) {
		writer.Write(data[i]);
	}

	// write child block pointer
	writer.Write(child_block_pointer.block_id);
	writer.Write(child_block_pointer.offset);

	return block_pointer;
}

void Prefix::Deserialize(MetaBlockReader &reader) {

	data[Node::PREFIX_SIZE] = reader.Read<uint8_t>();

	// read bytes
	for (idx_t i = 0; i < data[Node::PREFIX_SIZE]; i++) {
		data[i] = reader.Read<uint8_t>();
	}

	// read child block pointer
	ptr = Node(reader);
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
	D_ASSERT(other_prefix.IsSet() && !other_prefix.IsSwizzled());

	reference<Prefix> prefix(*this);
	while (other_prefix.DecodeARTNodeType() == NType::PREFIX) {

		// copy prefix bytes
		auto &other = Prefix::Get(art, other_prefix);
		for (idx_t i = 0; i < other.data[Node::PREFIX_SIZE]; i++) {
			prefix = prefix.get().Append(art, other.data[i]);
		}

		D_ASSERT(other.ptr.IsSet());
		if (other.ptr.IsSwizzled()) {
			other.ptr.Deserialize(art);
		}

		prefix.get().ptr = other.ptr;
		Node::GetAllocator(art, NType::PREFIX).Free(other_prefix);
		other_prefix = prefix.get().ptr;
	}

	D_ASSERT(prefix.get().ptr.DecodeARTNodeType() != NType::PREFIX);
}

} // namespace duckdb
