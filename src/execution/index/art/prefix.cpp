#include "duckdb/execution/index/art/prefix.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/storage/meta_block_reader.hpp"
#include "duckdb/storage/meta_block_writer.hpp"

namespace duckdb {

void Prefix::New(ART &art, reference<Node> &node, const ARTKey &key, const uint32_t depth, uint32_t count) {

	if (count == 0) {
		return;
	}
	idx_t copy_count = 0;

	while (count) {
		node.get().SetPtr(Node::GetAllocator(art, NType::PREFIX).New());
		node.get().type = (uint8_t)NType::PREFIX;
		auto &prefix = Prefix::Get(art, node);

		auto this_count = MinValue(uint32_t(Node::PREFIX_SIZE - 1), count);
		prefix.data[0] = (uint8_t)this_count;
		memcpy(prefix.data + 1, key.data + depth + copy_count, this_count);

		node = prefix.ptr;
		copy_count += this_count;
		count -= this_count;
	}
}

void Prefix::Free(ART &art, Node &node) {

	D_ASSERT(node.IsSet());
	D_ASSERT(!node.IsSwizzled());

	// free child node
	auto &child = Prefix::Get(art, node).ptr;
	Node::Free(art, child);
}

void Prefix::Concatenate(ART &art, Node &prefix, const uint8_t byte, Node &child_prefix) {
	// TODO
}

idx_t Prefix::Traverse(const ART &art, reference<Node> &l_node, reference<Node> &r_node) {
	// TODO
	return 0;
}

idx_t Prefix::Traverse(const ART &art, reference<Node> &prefix, const ARTKey &key, idx_t &depth) {
	// TODO
	return 0;
}

uint8_t Prefix::GetByte(const ART &art, const Node &prefix, const idx_t position) {
	// TODO
	return 0;
}

void Prefix::Reduce(ART &art, Node &prefix, const idx_t n) {
	// TODO
}

void Prefix::Split(ART &art, reference<Node> &prefix, Node &child, idx_t position) {
	// TODO
}

BlockPointer Prefix::Serialize(ART &art, MetaBlockWriter &writer) {

	// recurse into the child and retrieve its block pointer
	auto child_block_pointer = ptr.Serialize(art, writer);

	// get pointer and write fields
	auto block_pointer = writer.GetBlockPointer();
	writer.Write(NType::PREFIX);

	// write count and prefix bytes
	for (idx_t i = 0; i <= data[0]; i++) {
		writer.Write(data[0]);
	}

	// write child block pointer
	writer.Write(child_block_pointer.block_id);
	writer.Write(child_block_pointer.offset);

	return block_pointer;
}

void Prefix::Deserialize(MetaBlockReader &reader) {

	data[0] = reader.Read<uint8_t>();

	// read bytes
	for (idx_t i = 0; i < data[0]; i++) {
		data[i + 1] = reader.Read<uint8_t>();
	}

	// read child block pointer
	ptr = Node(reader);
}

} // namespace duckdb
