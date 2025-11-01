#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/art_scanner.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// New and free
//===--------------------------------------------------------------------===//

void Node::New(ART &art, Node &node, NType type) {
	switch (type) {
	case NType::NODE_7_LEAF:
		Node7Leaf::New(art, node);
		break;
	case NType::NODE_15_LEAF:
		Node15Leaf::New(art, node);
		break;
	case NType::NODE_256_LEAF:
		Node256Leaf::New(art, node);
		break;
	case NType::NODE_4:
		Node4::New(art, node);
		break;
	case NType::NODE_16:
		Node16::New(art, node);
		break;
	case NType::NODE_48:
		Node48::New(art, node);
		break;
	case NType::NODE_256:
		Node256::New(art, node);
		break;
	default:
		throw InternalException("Invalid node type for New: %d.", type);
	}
}

void Node::FreeNode(ART &art, Node &node) {
	D_ASSERT(node.HasMetadata());
	GetAllocator(art, node.GetType()).Free(node);
	node.Clear();
}

void Node::FreeTree(ART &art, Node &node) {
	auto handler = [&art](Node &node) {
		const auto type = node.GetType();
		switch (type) {
		case NType::LEAF_INLINED:
			node.Clear();
			return ARTHandlingResult::NONE;
		case NType::LEAF:
			Leaf::DeprecatedFree(art, node);
			return ARTHandlingResult::NONE;
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
		case NType::PREFIX:
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256:
			break;
		default:
			throw InternalException("invalid node type for Free: %d", type);
		}

		FreeNode(art, node);
		return ARTHandlingResult::NONE;
	};

	ARTScanner<ARTScanHandling::POP, Node> scanner(art, handler, node);
	scanner.Scan(handler);
}

//===--------------------------------------------------------------------===//
// Allocators
//===--------------------------------------------------------------------===//

FixedSizeAllocator &Node::GetAllocator(const ART &art, const NType type) {
	return *(*art.allocators)[GetAllocatorIdx(type)];
}

uint8_t Node::GetAllocatorIdx(const NType type) {
	switch (type) {
	case NType::PREFIX:
		return 0;
	case NType::LEAF:
		return 1;
	case NType::NODE_4:
		return 2;
	case NType::NODE_16:
		return 3;
	case NType::NODE_48:
		return 4;
	case NType::NODE_256:
		return 5;
	case NType::NODE_7_LEAF:
		return 6;
	case NType::NODE_15_LEAF:
		return 7;
	case NType::NODE_256_LEAF:
		return 8;
	default:
		throw InternalException("Invalid node type for GetAllocatorIdx: %d.", type);
	}
}

//===--------------------------------------------------------------------===//
// Inserts
//===--------------------------------------------------------------------===//

void Node::ReplaceChild(const ART &art, const uint8_t byte, const Node child) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::ReplaceChild(Ref<Node4>(art, *this, type), byte, child);
	case NType::NODE_16:
		return Node16::ReplaceChild(Ref<Node16>(art, *this, type), byte, child);
	case NType::NODE_48:
		return Ref<Node48>(art, *this, type).ReplaceChild(byte, child);
	case NType::NODE_256:
		return Ref<Node256>(art, *this, type).ReplaceChild(byte, child);
	default:
		throw InternalException("Invalid node type for ReplaceChild: %d.", type);
	}
}

void Node::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::InsertChild(art, node, byte, child);
	case NType::NODE_16:
		return Node16::InsertChild(art, node, byte, child);
	case NType::NODE_48:
		return Node48::InsertChild(art, node, byte, child);
	case NType::NODE_256:
		return Node256::InsertChild(art, node, byte, child);
	case NType::NODE_7_LEAF:
		return Node7Leaf::InsertByte(art, node, byte);
	case NType::NODE_15_LEAF:
		return Node15Leaf::InsertByte(art, node, byte);
	case NType::NODE_256_LEAF:
		return Node256Leaf::InsertByte(art, node, byte);
	default:
		throw InternalException("Invalid node type for InsertChild: %d.", type);
	}
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//

void Node::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const GateStatus status,
                       const ARTKey &row_id) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::DeleteChild(art, node, prefix, byte, status);
	case NType::NODE_16:
		return Node16::DeleteChild(art, node, byte);
	case NType::NODE_48:
		return Node48::DeleteChild(art, node, byte);
	case NType::NODE_256:
		return Node256::DeleteChild(art, node, byte);
	case NType::NODE_7_LEAF:
		return Node7Leaf::DeleteByte(art, node, prefix, byte, row_id);
	case NType::NODE_15_LEAF:
		return Node15Leaf::DeleteByte(art, node, byte);
	case NType::NODE_256_LEAF:
		return Node256Leaf::DeleteByte(art, node, byte);
	default:
		throw InternalException("Invalid node type for DeleteChild: %d.", type);
	}
}

//===--------------------------------------------------------------------===//
// Get child and byte.
//===--------------------------------------------------------------------===//

template <class NODE>
static unsafe_optional_ptr<Node> GetChildInternal(ART &art, NODE &node, const uint8_t byte, const bool unsafe) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::GetChild(Node::Ref<Node4>(art, node, type), byte, unsafe);
	case NType::NODE_16:
		return Node16::GetChild(Node::Ref<Node16>(art, node, type), byte, unsafe);
	case NType::NODE_48:
		return Node48::GetChild(Node::Ref<Node48>(art, node, type), byte, unsafe);
	case NType::NODE_256: {
		return Node256::GetChild(Node::Ref<Node256>(art, node, type), byte, unsafe);
	}
	default:
		throw InternalException("Invalid node type for GetChildInternal: %d.", type);
	}
}

const unsafe_optional_ptr<Node> Node::GetChild(ART &art, const uint8_t byte) const {
	return GetChildInternal(art, *this, byte, false);
}

unsafe_optional_ptr<Node> Node::GetChildMutable(ART &art, const uint8_t byte, const bool unsafe) const {
	return GetChildInternal(art, *this, byte, unsafe);
}

template <class NODE>
unsafe_optional_ptr<Node> GetNextChildInternal(ART &art, NODE &node, uint8_t &byte) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::GetNextChild(Node::Ref<Node4>(art, node, type), byte);
	case NType::NODE_16:
		return Node16::GetNextChild(Node::Ref<Node16>(art, node, type), byte);
	case NType::NODE_48:
		return Node48::GetNextChild(Node::Ref<Node48>(art, node, type), byte);
	case NType::NODE_256:
		return Node256::GetNextChild(Node::Ref<Node256>(art, node, type), byte);
	default:
		throw InternalException("Invalid node type for GetNextChildInternal: %d.", type);
	}
}

const unsafe_optional_ptr<Node> Node::GetNextChild(ART &art, uint8_t &byte) const {
	return GetNextChildInternal(art, *this, byte);
}

bool Node::HasByte(ART &art, const uint8_t byte) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::NODE_7_LEAF:
		return Ref<const Node7Leaf>(art, *this, NType::NODE_7_LEAF).HasByte(byte);
	case NType::NODE_15_LEAF:
		return Ref<const Node15Leaf>(art, *this, NType::NODE_15_LEAF).HasByte(byte);
	case NType::NODE_256_LEAF:
		return Ref<Node256Leaf>(art, *this, NType::NODE_256_LEAF).HasByte(byte);
	default:
		throw InternalException("Invalid node type for GetNextByte: %d.", type);
	}
}

bool Node::GetNextByte(ART &art, uint8_t &byte) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::NODE_7_LEAF:
		return Ref<const Node7Leaf>(art, *this, NType::NODE_7_LEAF).GetNextByte(byte);
	case NType::NODE_15_LEAF:
		return Ref<const Node15Leaf>(art, *this, NType::NODE_15_LEAF).GetNextByte(byte);
	case NType::NODE_256_LEAF:
		return Ref<Node256Leaf>(art, *this, NType::NODE_256_LEAF).GetNextByte(byte);
	default:
		throw InternalException("Invalid node type for GetNextByte: %d.", type);
	}
}

//===--------------------------------------------------------------------===//
// Utility
//===--------------------------------------------------------------------===//

idx_t GetCapacity(NType type) {
	switch (type) {
	case NType::NODE_4:
		return Node4::CAPACITY;
	case NType::NODE_7_LEAF:
		return Node7Leaf::CAPACITY;
	case NType::NODE_15_LEAF:
		return Node15Leaf::CAPACITY;
	case NType::NODE_16:
		return Node16::CAPACITY;
	case NType::NODE_48:
		return Node48::CAPACITY;
	case NType::NODE_256_LEAF:
		return Node256::CAPACITY;
	case NType::NODE_256:
		return Node256::CAPACITY;
	default:
		throw InternalException("Invalid node type for GetCapacity: %d.", type);
	}
}

NType Node::GetNodeType(idx_t count) {
	if (count <= Node4::CAPACITY) {
		return NType::NODE_4;
	} else if (count <= Node16::CAPACITY) {
		return NType::NODE_16;
	} else if (count <= Node48::CAPACITY) {
		return NType::NODE_48;
	}
	return NType::NODE_256;
}

bool Node::IsNode() const {
	switch (GetType()) {
	case NType::NODE_4:
	case NType::NODE_16:
	case NType::NODE_48:
	case NType::NODE_256:
		return true;
	default:
		return false;
	}
}

bool Node::IsLeafNode() const {
	switch (GetType()) {
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		return true;
	default:
		return false;
	}
}

bool Node::IsAnyLeaf() const {
	if (IsLeafNode()) {
		return true;
	}

	switch (GetType()) {
	case NType::LEAF_INLINED:
	case NType::LEAF:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// TransformToDeprecated
//===--------------------------------------------------------------------===//

void Node::TransformToDeprecated(ART &art, Node &node,
                                 unsafe_unique_ptr<FixedSizeAllocator> &deprecated_prefix_allocator) {
	D_ASSERT(node.HasMetadata());

	if (node.GetGateStatus() == GateStatus::GATE_SET) {
		D_ASSERT(node.GetType() != NType::LEAF_INLINED);
		return Leaf::TransformToDeprecated(art, node);
	}

	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX:
		return Prefix::TransformToDeprecated(art, node, deprecated_prefix_allocator);
	case NType::LEAF_INLINED:
		return;
	case NType::LEAF:
		return;
	case NType::NODE_4:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node4>(art, node, type), deprecated_prefix_allocator);
	case NType::NODE_16:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node16>(art, node, type), deprecated_prefix_allocator);
	case NType::NODE_48:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node48>(art, node, type), deprecated_prefix_allocator);
	case NType::NODE_256:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node256>(art, node, type), deprecated_prefix_allocator);
	default:
		throw InternalException("invalid node type for TransformToDeprecated: %d", type);
	}
}

//===--------------------------------------------------------------------===//
// Verification
//===--------------------------------------------------------------------===//

void Node::Verify(ART &art) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::LEAF_INLINED:
		return;
	case NType::LEAF:
		Leaf::DeprecatedVerify(art, *this);
		return;
	case NType::PREFIX: {
		Prefix::Verify(art, *this);
		return;
	}
	default:
		break;
	}

	if (!IsLeafNode()) {
		uint8_t byte = 0;
		auto child = GetNextChild(art, byte);
		while (child) {
			child->Verify(art);
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			child = GetNextChild(art, byte);
		}
	}
}

void Node::VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const {
	D_ASSERT(HasMetadata());

	auto handler = [&art, &node_counts](const Node &node) {
		ARTHandlingResult result;
		const auto type = node.GetType();
		switch (type) {
		case NType::LEAF_INLINED:
			return ARTHandlingResult::SKIP;
		case NType::LEAF: {
			auto &leaf = Ref<Leaf>(art, node, type);
			leaf.DeprecatedVerifyAllocations(art, node_counts);
			return ARTHandlingResult::SKIP;
		}
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF: {
			result = ARTHandlingResult::SKIP;
			break;
		}
		case NType::PREFIX:
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256: {
			result = ARTHandlingResult::CONTINUE;
			break;
		}
		default:
			throw InternalException("invalid node type for VerifyAllocations: %d", type);
		}
		node_counts[GetAllocatorIdx(type)]++;
		return result;
	};

	ARTScanner<ARTScanHandling::EMPLACE, const Node> scanner(art, handler, *this);
	scanner.Scan(handler);
}

//===--------------------------------------------------------------------===//
// Printing
//===--------------------------------------------------------------------===//

string Node::ToString(ART &art, idx_t indent_level, bool inside_gate, bool display_ascii) const {
	auto indent = [](string &str, const idx_t n) {
		for (idx_t i = 0; i < n; ++i) {
			str += " ";
		}
	};
	// if inside gate, print byte values not ascii.
	auto format_byte = [&](uint8_t byte) {
		if (!inside_gate && display_ascii && byte >= 32 && byte <= 126) {
			return string(1, static_cast<char>(byte));
		}
		return to_string(byte);
	};
	auto type = GetType();
	bool is_gate = GetGateStatus() == GateStatus::GATE_SET;
	bool propagate_gate = inside_gate || is_gate;

	switch (type) {
	case NType::LEAF_INLINED: {
		string str = "";
		indent(str, indent_level);
		return str + "Inlined Leaf [row ID: " + to_string(GetRowId()) + "]\n";
	}
	case NType::LEAF:
		return Leaf::DeprecatedToString(art, *this);
	case NType::PREFIX: {
		string str = Prefix::ToString(art, *this, indent_level, propagate_gate, display_ascii);
		if (is_gate) {
			string s = "";
			indent(s, indent_level);
			s += "Gate\n";
			return s + str;
		}
		string s = "";
		return s + str;
	}
	default:
		break;
	}
	string str = "";
	indent(str, indent_level);
	str = str + "Node" + to_string(GetCapacity(type)) += "\n";
	uint8_t byte = 0;

	if (IsLeafNode()) {
		indent(str, indent_level);
		str += "Leaf |";
		auto has_byte = GetNextByte(art, byte);
		while (has_byte) {
			str += format_byte(byte) + "|";
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			has_byte = GetNextByte(art, byte);
		}
		str += "\n";
	} else {
		auto child = GetNextChild(art, byte);
		while (child) {
			string c = child->ToString(art, indent_level + 2, propagate_gate, display_ascii);
			indent(str, indent_level);
			str = str + format_byte(byte) + ",\n" + c;
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			child = GetNextChild(art, byte);
		}
	}

	if (is_gate) {
		string s = "";
		indent(s, indent_level + 2);
		str = "Gate\n" + s + str;
	}
	return str;
}

} // namespace duckdb
