#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/iterator.hpp"
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
		throw InternalException("Invalid node type for New: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for GetAllocatorIdx: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for ReplaceChild: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for InsertChild: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for DeleteChild: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for GetChildInternal: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for GetNextChildInternal: %d.", static_cast<uint8_t>(type));
	}
}

const unsafe_optional_ptr<Node> Node::GetNextChild(ART &art, uint8_t &byte) const {
	return GetNextChildInternal(art, *this, byte);
}

unsafe_optional_ptr<Node> Node::GetNextChildMutable(ART &art, uint8_t &byte) const {
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
		throw InternalException("Invalid node type for GetNextByte: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for GetNextByte: %d.", static_cast<uint8_t>(type));
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
		throw InternalException("Invalid node type for GetCapacity: %d.", static_cast<uint8_t>(type));
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
// Merge
//===--------------------------------------------------------------------===//

void Node::InitMerge(ART &art, const unsafe_vector<idx_t> &upper_bounds) {
	D_ASSERT(HasMetadata());
	auto type = GetType();

	switch (type) {
	case NType::PREFIX:
		return Prefix::InitializeMerge(art, *this, upper_bounds);
	case NType::LEAF:
		throw InternalException("Failed to initialize merge due to deprecated ART storage.");
	case NType::NODE_4:
		InitMergeInternal(art, Ref<Node4>(art, *this, type), upper_bounds);
		break;
	case NType::NODE_16:
		InitMergeInternal(art, Ref<Node16>(art, *this, type), upper_bounds);
		break;
	case NType::NODE_48:
		InitMergeInternal(art, Ref<Node48>(art, *this, type), upper_bounds);
		break;
	case NType::NODE_256:
		InitMergeInternal(art, Ref<Node256>(art, *this, type), upper_bounds);
		break;
	case NType::LEAF_INLINED:
		return;
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		break;
	}

	auto idx = GetAllocatorIdx(type);
	IncreaseBufferId(upper_bounds[idx]);
}

bool Node::MergeNormalNodes(ART &art, Node &l_node, Node &r_node, uint8_t &byte, const GateStatus status) {
	// Merge N4, N16, N48, N256 nodes.
	D_ASSERT(l_node.IsNode() && r_node.IsNode());
	D_ASSERT(l_node.GetGateStatus() == r_node.GetGateStatus());

	auto r_child = r_node.GetNextChildMutable(art, byte);
	while (r_child) {
		auto l_child = l_node.GetChildMutable(art, byte);
		if (!l_child) {
			Node::InsertChild(art, l_node, byte, *r_child);
			r_node.ReplaceChild(art, byte);
		} else {
			if (!l_child->MergeInternal(art, *r_child, status)) {
				return false;
			}
		}

		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		r_child = r_node.GetNextChildMutable(art, byte);
	}

	Node::Free(art, r_node);
	return true;
}

void Node::MergeLeafNodes(ART &art, Node &l_node, Node &r_node, uint8_t &byte) {
	// Merge N7, N15, N256 leaf nodes.
	D_ASSERT(l_node.IsLeafNode() && r_node.IsLeafNode());
	D_ASSERT(l_node.GetGateStatus() == GateStatus::GATE_NOT_SET);
	D_ASSERT(r_node.GetGateStatus() == GateStatus::GATE_NOT_SET);

	auto has_next = r_node.GetNextByte(art, byte);
	while (has_next) {
		// Row IDs are always unique.
		Node::InsertChild(art, l_node, byte);
		if (byte == NumericLimits<uint8_t>::Maximum()) {
			break;
		}
		byte++;
		has_next = r_node.GetNextByte(art, byte);
	}

	Node::Free(art, r_node);
}

bool Node::MergeNodes(ART &art, Node &other, GateStatus status) {
	// Merge the smaller node into the bigger node.
	if (GetType() < other.GetType()) {
		swap(*this, other);
	}

	uint8_t byte = 0;
	if (IsNode()) {
		return MergeNormalNodes(art, *this, other, byte, status);
	}
	MergeLeafNodes(art, *this, other, byte);
	return true;
}

bool Node::Merge(ART &art, Node &other, const GateStatus status) {
	if (HasMetadata()) {
		return MergeInternal(art, other, status);
	}

	*this = other;
	other = Node();
	return true;
}

bool Node::PrefixContainsOther(ART &art, Node &l_node, Node &r_node, const uint8_t pos, const GateStatus status) {
	// r_node's prefix contains l_node's prefix. l_node must be a node with child nodes.
	D_ASSERT(l_node.IsNode());

	// Check if the next byte (pos) in r_node exists in l_node.
	auto byte = Prefix::GetByte(art, r_node, pos);
	auto child = l_node.GetChildMutable(art, byte);

	// Reduce r_node's prefix to the bytes after pos.
	Prefix::Reduce(art, r_node, pos);
	if (child) {
		return child->MergeInternal(art, r_node, status);
	}

	Node::InsertChild(art, l_node, byte, r_node);
	r_node.Clear();
	return true;
}

void Node::MergeIntoNode4(ART &art, Node &l_node, Node &r_node, const uint8_t pos) {
	Node l_child;
	auto l_byte = Prefix::GetByte(art, l_node, pos);

	reference<Node> ref(l_node);
	auto status = Prefix::Split(art, ref, l_child, pos);
	Node4::New(art, ref);
	ref.get().SetGateStatus(status);

	Node4::InsertChild(art, ref, l_byte, l_child);

	auto r_byte = Prefix::GetByte(art, r_node, pos);
	Prefix::Reduce(art, r_node, pos);
	Node4::InsertChild(art, ref, r_byte, r_node);
	r_node.Clear();
}

bool Node::MergePrefixes(ART &art, Node &other, const GateStatus status) {
	reference<Node> l_node(*this);
	reference<Node> r_node(other);
	auto pos = DConstants::INVALID_INDEX;

	if (l_node.get().GetType() == NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
		// Traverse prefixes. Possibly change the referenced nodes.
		if (!Prefix::Traverse(art, l_node, r_node, pos, status)) {
			return false;
		}
		if (pos == DConstants::INVALID_INDEX) {
			return true;
		}

	} else {
		// l_prefix contains r_prefix.
		if (l_node.get().GetType() == NType::PREFIX) {
			swap(*this, other);
		}
		pos = 0;
	}

	D_ASSERT(pos != DConstants::INVALID_INDEX);
	if (l_node.get().GetType() != NType::PREFIX && r_node.get().GetType() == NType::PREFIX) {
		return PrefixContainsOther(art, l_node, r_node, UnsafeNumericCast<uint8_t>(pos), status);
	}

	// The prefixes differ.
	MergeIntoNode4(art, l_node, r_node, UnsafeNumericCast<uint8_t>(pos));
	return true;
}

bool Node::MergeInternal(ART &art, Node &other, const GateStatus status) {
	D_ASSERT(HasMetadata());
	D_ASSERT(other.HasMetadata());

	// Merge inlined leaves.
	if (GetType() == NType::LEAF_INLINED) {
		swap(*this, other);
	}
	if (other.GetType() == NType::LEAF_INLINED) {
		D_ASSERT(status == GateStatus::GATE_NOT_SET);
		D_ASSERT(other.GetGateStatus() == GateStatus::GATE_SET || other.GetType() == NType::LEAF_INLINED);
		D_ASSERT(GetType() == NType::LEAF_INLINED || GetGateStatus() == GateStatus::GATE_SET);

		if (art.IsUnique()) {
			return false;
		}
		Leaf::MergeInlined(art, *this, other);
		return true;
	}

	// Enter a gate.
	if (GetGateStatus() == GateStatus::GATE_SET && status == GateStatus::GATE_NOT_SET) {
		D_ASSERT(other.GetGateStatus() == GateStatus::GATE_SET);
		D_ASSERT(GetType() != NType::LEAF_INLINED);
		D_ASSERT(other.GetType() != NType::LEAF_INLINED);

		// Get all row IDs.
		unsafe_vector<row_t> row_ids;
		Iterator it(art);
		it.FindMinimum(other);
		ARTKey empty_key = ARTKey();
		it.Scan(empty_key, NumericLimits<row_t>().Maximum(), row_ids, false);
		Node::Free(art, other);
		D_ASSERT(row_ids.size() > 1);

		// Insert all row IDs.
		ArenaAllocator allocator(Allocator::Get(art.db));
		for (idx_t i = 0; i < row_ids.size(); i++) {
			auto row_id = ARTKey::CreateARTKey<row_t>(allocator, row_ids[i]);
			art.Insert(*this, row_id, 0, row_id, GateStatus::GATE_SET);
		}
		return true;
	}

	// Merge N4, N16, N48, N256 nodes.
	if (IsNode() && other.IsNode()) {
		return MergeNodes(art, other, status);
	}
	// Merge N7, N15, N256 leaf nodes.
	if (IsLeafNode() && other.IsLeafNode()) {
		D_ASSERT(status == GateStatus::GATE_SET);
		return MergeNodes(art, other, status);
	}

	// Merge prefixes.
	return MergePrefixes(art, other, status);
}

//===--------------------------------------------------------------------===//
// Vacuum
//===--------------------------------------------------------------------===//

void Node::Vacuum(ART &art, const unordered_set<uint8_t> &indexes) {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::LEAF_INLINED:
		return;
	case NType::PREFIX:
		return Prefix::Vacuum(art, *this, indexes);
	case NType::LEAF:
		if (indexes.find(GetAllocatorIdx(type)) == indexes.end()) {
			return;
		}
		return Leaf::DeprecatedVacuum(art, *this);
	default:
		break;
	}

	auto idx = GetAllocatorIdx(type);
	auto &allocator = GetAllocator(art, type);
	auto needs_vacuum = indexes.find(idx) != indexes.end() && allocator.NeedsVacuum(*this);
	if (needs_vacuum) {
		auto status = GetGateStatus();
		*this = allocator.VacuumPointer(*this);
		SetMetadata(static_cast<uint8_t>(type));
		SetGateStatus(status);
	}

	switch (type) {
	case NType::NODE_4:
		return VacuumInternal(art, Ref<Node4>(art, *this, type), indexes);
	case NType::NODE_16:
		return VacuumInternal(art, Ref<Node16>(art, *this, type), indexes);
	case NType::NODE_48:
		return VacuumInternal(art, Ref<Node48>(art, *this, type), indexes);
	case NType::NODE_256:
		return VacuumInternal(art, Ref<Node256>(art, *this, type), indexes);
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		return;
	default:
		throw InternalException("Invalid node type for Vacuum: %d.", static_cast<uint8_t>(type));
	}
}

//===--------------------------------------------------------------------===//
// TransformToDeprecated
//===--------------------------------------------------------------------===//

void Node::TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
	D_ASSERT(node.HasMetadata());

	if (node.GetGateStatus() == GateStatus::GATE_SET) {
		return Leaf::TransformToDeprecated(art, node);
	}

	auto type = node.GetType();
	switch (type) {
	case NType::PREFIX:
		return Prefix::TransformToDeprecated(art, node, allocator);
	case NType::LEAF_INLINED:
		return;
	case NType::LEAF:
		return;
	case NType::NODE_4:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node4>(art, node, type), allocator);
	case NType::NODE_16:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node16>(art, node, type), allocator);
	case NType::NODE_48:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node48>(art, node, type), allocator);
	case NType::NODE_256:
		return TransformToDeprecatedInternal(art, InMemoryRef<Node256>(art, node, type), allocator);
	default:
		throw InternalException("Invalid node type for TransformToDeprecated: %d.", static_cast<uint8_t>(type));
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

	auto type = GetType();
	switch (type) {
	case NType::PREFIX:
		return Prefix::VerifyAllocations(art, *this, node_counts);
	case NType::LEAF:
		return Ref<Leaf>(art, *this, type).DeprecatedVerifyAllocations(art, node_counts);
	case NType::LEAF_INLINED:
		return;
	case NType::NODE_4:
		VerifyAllocationsInternal(art, Ref<Node4>(art, *this, type), node_counts);
		break;
	case NType::NODE_16:
		VerifyAllocationsInternal(art, Ref<Node16>(art, *this, type), node_counts);
		break;
	case NType::NODE_48:
		VerifyAllocationsInternal(art, Ref<Node48>(art, *this, type), node_counts);
		break;
	case NType::NODE_256:
		VerifyAllocationsInternal(art, Ref<Node256>(art, *this, type), node_counts);
		break;
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		break;
	}

	node_counts[GetAllocatorIdx(type)]++;
}

} // namespace duckdb
