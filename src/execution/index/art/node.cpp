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
		throw InternalException("Invalid node type for New: %s.", EnumUtil::ToString(type));
	}
}

void Node::Free(ART &art, Node &node) {
	if (!node.HasMetadata()) {
		return node.Clear();
	}

	// Free the children.
	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX:
		return Prefix::Free(art, node);
	case NType::LEAF:
		return Leaf::DeprecatedFree(art, node);
	case NType::NODE_4:
		Node4::Free(art, node);
		break;
	case NType::NODE_16:
		Node16::Free(art, node);
		break;
	case NType::NODE_48:
		Node48::Free(art, node);
		break;
	case NType::NODE_256:
		Node256::Free(art, node);
		break;
	case NType::LEAF_INLINED:
		return node.Clear();
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		break;
	}

	GetAllocator(art, type).Free(node);
	node.Clear();
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
		throw InternalException("Invalid node type for GetAllocatorIdx: %s.", EnumUtil::ToString(type));
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
		throw InternalException("Invalid node type for ReplaceChild: %s.", EnumUtil::ToString(type));
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
		throw InternalException("Invalid node type for InsertChild: %s.", EnumUtil::ToString(type));
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
		throw InternalException("Invalid node type for DeleteChild: %s.", EnumUtil::ToString(type));
	}
}

//===--------------------------------------------------------------------===//
// Get child and byte.
//===--------------------------------------------------------------------===//

template <class NODE>
unsafe_optional_ptr<Node> GetChildInternal(ART &art, NODE &node, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::GetChild(Node::Ref<Node4>(art, node, type), byte);
	case NType::NODE_16:
		return Node16::GetChild(Node::Ref<Node16>(art, node, type), byte);
	case NType::NODE_48:
		return Node48::GetChild(Node::Ref<Node48>(art, node, type), byte);
	case NType::NODE_256: {
		return Node256::GetChild(Node::Ref<Node256>(art, node, type), byte);
	}
	default:
		throw InternalException("Invalid node type for GetChildInternal: %s.", EnumUtil::ToString(type));
	}
}

const unsafe_optional_ptr<Node> Node::GetChild(ART &art, const uint8_t byte) const {
	return GetChildInternal(art, *this, byte);
}

unsafe_optional_ptr<Node> Node::GetChildMutable(ART &art, const uint8_t byte) const {
	return GetChildInternal(art, *this, byte);
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
		throw InternalException("Invalid node type for GetNextChildInternal: %s.", EnumUtil::ToString(type));
	}
}

const unsafe_optional_ptr<Node> Node::GetNextChild(ART &art, uint8_t &byte) const {
	return GetNextChildInternal(art, *this, byte);
}

bool Node::HasByte(ART &art, uint8_t &byte) const {
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
		throw InternalException("Invalid node type for GetNextByte: %s.", EnumUtil::ToString(type));
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
		throw InternalException("Invalid node type for GetNextByte: %s.", EnumUtil::ToString(type));
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
		throw InternalException("Invalid node type for GetCapacity: %s.", EnumUtil::ToString(type));
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
		throw InternalException("invalid node type for TransformToDeprecated: %s", EnumUtil::ToString(type));
	}
}

//===--------------------------------------------------------------------===//
// Verification
//===--------------------------------------------------------------------===//

string Node::VerifyAndToString(ART &art, const bool only_verify) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::LEAF_INLINED:
		return only_verify ? "" : "Inlined Leaf [row ID: " + to_string(GetRowId()) + "]";
	case NType::LEAF:
		return Leaf::DeprecatedVerifyAndToString(art, *this, only_verify);
	case NType::PREFIX: {
		auto str = Prefix::VerifyAndToString(art, *this, only_verify);
		if (GetGateStatus() == GateStatus::GATE_SET) {
			str = "Gate [ " + str + " ]";
		}
		return only_verify ? "" : "\n" + str;
	}
	default:
		break;
	}

	string str = "Node" + to_string(GetCapacity(type)) + ": [ ";
	uint8_t byte = 0;

	if (IsLeafNode()) {
		str = "Leaf " + str;
		auto has_byte = GetNextByte(art, byte);
		while (has_byte) {
			str += to_string(byte) + "-";
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			has_byte = GetNextByte(art, byte);
		}
	} else {
		auto child = GetNextChild(art, byte);
		while (child) {
			str += "(" + to_string(byte) + ", " + child->VerifyAndToString(art, only_verify) + ")";
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			child = GetNextChild(art, byte);
		}
	}

	if (GetGateStatus() == GateStatus::GATE_SET) {
		str = "Gate [ " + str + " ]";
	}
	return only_verify ? "" : "\n" + str + "]";
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
			throw InternalException("invalid node type for VerifyAllocations: %s", EnumUtil::ToString(type));
		}
		node_counts[GetAllocatorIdx(type)]++;
		return result;
	};

	ARTScanner<ARTScanHandling::EMPLACE, const Node> scanner(art, handler, *this);
	scanner.Scan(handler);
}

} // namespace duckdb
