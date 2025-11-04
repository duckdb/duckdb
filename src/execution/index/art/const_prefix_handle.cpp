#include "duckdb/execution/index/art/const_prefix_handle.hpp"

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

ConstPrefixHandle::ConstPrefixHandle(const ART &art, const Node node)
    : segment_handle(make_uniq<SegmentHandle>(Node::GetAllocator(art, PREFIX).GetHandle(node))) {
	data = segment_handle->GetPtr();
	ptr = reinterpret_cast<Node *>(data + Count(art) + 1);
	// Read-only: don't mark segment as modified
}

uint8_t ConstPrefixHandle::GetCount(const ART &art) const {
	return data[Count(art)];
}

uint8_t ConstPrefixHandle::GetByte(const idx_t pos) const {
	return data[pos];
}

string ConstPrefixHandle::ToString(ART &art, const Node &node, idx_t indent_level, const bool inside_gate,
                                   const bool display_ascii) {
	auto indent = [](string &str, const idx_t n) {
		for (idx_t i = 0; i < n; ++i) {
			str += " ";
		}
	};
	auto format_byte = [&](const uint8_t byte) {
		if (!inside_gate && display_ascii && byte >= 32 && byte <= 126) {
			return string(1, static_cast<char>(byte));
		}
		return to_string(byte);
	};
	string str = "";
	indent(str, indent_level);
	reference<const Node> ref(node);
	Iterator(art, ref, true, [&](const ConstPrefixHandle &handle) {
		str += "Prefix: |";
		for (idx_t i = 0; i < handle.GetCount(art); i++) {
			str += format_byte(handle.data[i]) + "|";
		}
	});

	auto child = ref.get().ToString(art, indent_level, inside_gate, display_ascii);
	return str + "\n" + child;
}

void ConstPrefixHandle::Verify(ART &art, const Node &node) {
	reference<const Node> ref(node);

	Iterator(art, ref, true, [&](const ConstPrefixHandle &handle) {
		D_ASSERT(handle.GetCount(art) != 0);
		D_ASSERT(handle.GetCount(art) <= Count(art));
	});

	ref.get().Verify(art);
}

} // namespace duckdb
