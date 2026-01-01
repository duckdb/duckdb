#include "duckdb/execution/index/art/const_prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

ConstPrefixHandle::ConstPrefixHandle(const ART &art, const Node node)
    : segment_handle(Node::GetAllocator(art, PREFIX).GetHandle(node)) {
	data = segment_handle.GetPtr();
	child = reinterpret_cast<Node *>(data + art.PrefixCount() + 1);
	// Read-only: don't mark segment as modified
}

uint8_t ConstPrefixHandle::GetCount(const ART &art) const {
	return data[art.PrefixCount()];
}

uint8_t ConstPrefixHandle::GetByte(const idx_t pos) const {
	return data[pos];
}

namespace {
// Tree-style branch characters (UTF-8 encoded)
const string PREFIX_BRANCH_END = "\342\224\224\342\224\200\342\224\200 "; // "└── "
const string PREFIX_SPACE = "    ";

// ASCII printable character range
constexpr uint8_t PREFIX_ASCII_PRINTABLE_MIN = 32;
constexpr uint8_t PREFIX_ASCII_PRINTABLE_MAX = 126;
} // namespace

string ConstPrefixHandle::ToString(ART &art, const Node &node, const ToStringOptions &options) {
	auto format_byte = [&](const uint8_t byte) {
		if (!options.inside_gate && options.display_ascii && byte >= PREFIX_ASCII_PRINTABLE_MIN &&
		    byte <= PREFIX_ASCII_PRINTABLE_MAX) {
			return string(1, static_cast<char>(byte));
		}
		return to_string(byte);
	};

	// Print prefix bytes (single branch, child follows on next line)
	string str = options.tree_prefix + PREFIX_BRANCH_END + "Prefix: |";
	reference<const Node> ref(node);
	ToStringOptions child_options = options;
	Iterator(art, ref, true, [&](const ConstPrefixHandle &handle) {
		for (idx_t i = 0; i < handle.data[art.PrefixCount()]; i++) {
			str += format_byte(handle.data[i]) + "|";
			if (options.key_path) {
				child_options.key_depth++;
			}
		}
	});
	str += "\n";

	// Child is printed indented under the prefix (not as a sibling)
	child_options.tree_prefix = options.tree_prefix + PREFIX_SPACE;
	str += ref.get().ToString(art, child_options);
	return str;
}

void ConstPrefixHandle::Verify(ART &art, const Node &node) {
	reference<const Node> ref(node);

	Iterator(art, ref, true, [&](const ConstPrefixHandle &handle) {
		D_ASSERT(handle.data[art.PrefixCount()] != 0);
		D_ASSERT(handle.data[art.PrefixCount()] <= art.PrefixCount());
	});

	ref.get().Verify(art);
}

} // namespace duckdb
