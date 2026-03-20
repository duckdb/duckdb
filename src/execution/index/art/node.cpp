#include "duckdb/execution/index/art/node.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/swap.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art_scanner.hpp"
#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/const_prefix_handle.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"
#include "duckdb/storage/table_io_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// New and free
//===--------------------------------------------------------------------===//

void NodePointer::New(ART &art, NodePointer &node, NType type) {
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

void NodePointer::FreeNode(ART &art, NodePointer &node) {
	D_ASSERT(node.HasMetadata());
	GetAllocator(art, node.GetType()).Free(node);
	node.Clear();
}

void NodePointer::FreeTree(ART &art, NodePointer &tree) {
	if (!tree.HasMetadata()) {
		return;
	}

	// All nodes should be pushed onto the stack.
	auto filter = [](NodePointer &child) -> NodePointer {
		D_ASSERT(child.HasMetadata());
		return child;
	};

	// We freed the subtree pointed to by the current node. Free the node.
	auto post_handler = [&](NodePointer current) {
		D_ASSERT(current.HasMetadata());
		auto type = current.GetType();
		switch (type) {
		case NType::LEAF_INLINED:
			break;
		case NType::LEAF:
			Leaf::DeprecatedFree(art, current);
			break;
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
		case NType::PREFIX:
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256:
			FreeNode(art, current);
			break;
		default:
			throw InternalException("invalid node type for FreeTree: %d", static_cast<int>(type));
		}
	};

	ARTScanPostorder(art, tree, filter, post_handler);
	//
	tree.Clear();
}

//===--------------------------------------------------------------------===//
// Allocators
//===--------------------------------------------------------------------===//

FixedSizeAllocator &NodePointer::GetAllocator(const ART &art, const NType type) {
	return *(*art.allocators)[GetAllocatorIdx(type)];
}

uint8_t NodePointer::GetAllocatorIdx(const NType type) {
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

void NodePointer::ReplaceChild(const ART &art, const uint8_t byte, const NodePointer child) const {
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

void NodePointer::InsertChild(ART &art, NodePointer &node, const uint8_t byte, const NodePointer child) {
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

void NodePointer::DeleteChild(ART &art, NodePointer &node, NodePointer &prefix, const uint8_t byte,
                              const GateStatus status, const ARTKey &row_id) {
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
static unsafe_optional_ptr<NodePointer> GetChildInternal(ART &art, NODE &node, const uint8_t byte, const bool unsafe) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::GetChild(NodePointer::Ref<Node4>(art, node, type), byte, unsafe);
	case NType::NODE_16:
		return Node16::GetChild(NodePointer::Ref<Node16>(art, node, type), byte, unsafe);
	case NType::NODE_48:
		return Node48::GetChild(NodePointer::Ref<Node48>(art, node, type), byte, unsafe);
	case NType::NODE_256: {
		return Node256::GetChild(NodePointer::Ref<Node256>(art, node, type), byte, unsafe);
	}
	default:
		throw InternalException("Invalid node type for GetChildInternal: %d.", type);
	}
}

const unsafe_optional_ptr<NodePointer> NodePointer::GetChild(ART &art, const uint8_t byte) const {
	return GetChildInternal(art, *this, byte, false);
}

unsafe_optional_ptr<NodePointer> NodePointer::GetChildMutable(ART &art, const uint8_t byte, const bool unsafe) const {
	return GetChildInternal(art, *this, byte, unsafe);
}

NodePointer NodePointer::GetChildNode(const ART &art, const uint8_t byte) const {
	D_ASSERT(HasMetadata());
	auto type = GetType();
	ConstNodeHandle handle(art, *this);
	switch (type) {
	case NType::NODE_4:
		return Node4::GetChildNode(handle.Get<Node4>(), byte);
	case NType::NODE_16:
		return Node16::GetChildNode(handle.Get<Node16>(), byte);
	case NType::NODE_48:
		return Node48::GetChildNode(handle.Get<Node48>(), byte);
	case NType::NODE_256:
		return Node256::GetChildNode(handle.Get<Node256>(), byte);
	default:
		throw InternalException("Invalid node type for GetChildNode: %d.", type);
	}
}

NodePointer NodePointer::GetNextChildNode(const ART &art, uint8_t &byte) const {
	D_ASSERT(HasMetadata());
	auto type = GetType();
	ConstNodeHandle handle(art, *this);
	switch (type) {
	case NType::NODE_4:
		return Node4::GetNextChildNode(handle.Get<Node4>(), byte);
	case NType::NODE_16:
		return Node16::GetNextChildNode(handle.Get<Node16>(), byte);
	case NType::NODE_48:
		return Node48::GetNextChildNode(handle.Get<Node48>(), byte);
	case NType::NODE_256:
		return Node256::GetNextChildNode(handle.Get<Node256>(), byte);
	default:
		throw InternalException("Invalid node type for GetNextChildNode: %d.", type);
	}
}

template <class NODE>
unsafe_optional_ptr<NodePointer> GetNextChildInternal(ART &art, NODE &node, uint8_t &byte) {
	D_ASSERT(node.HasMetadata());

	auto type = node.GetType();
	switch (type) {
	case NType::NODE_4:
		return Node4::GetNextChild(NodePointer::Ref<Node4>(art, node, type), byte);
	case NType::NODE_16:
		return Node16::GetNextChild(NodePointer::Ref<Node16>(art, node, type), byte);
	case NType::NODE_48:
		return Node48::GetNextChild(NodePointer::Ref<Node48>(art, node, type), byte);
	case NType::NODE_256:
		return Node256::GetNextChild(NodePointer::Ref<Node256>(art, node, type), byte);
	default:
		throw InternalException("Invalid node type for GetNextChildInternal: %d.", type);
	}
}

const unsafe_optional_ptr<NodePointer> NodePointer::GetNextChild(ART &art, uint8_t &byte) const {
	return GetNextChildInternal(art, *this, byte);
}

bool NodePointer::HasByte(ART &art, const uint8_t byte) const {
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

bool NodePointer::GetNextByte(ART &art, uint8_t &byte) const {
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

NType NodePointer::GetNodeType(idx_t count) {
	if (count <= Node4::CAPACITY) {
		return NType::NODE_4;
	} else if (count <= Node16::CAPACITY) {
		return NType::NODE_16;
	} else if (count <= Node48::CAPACITY) {
		return NType::NODE_48;
	}
	return NType::NODE_256;
}

bool NodePointer::IsNode() const {
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

bool NodePointer::IsLeafNode() const {
	switch (GetType()) {
	case NType::NODE_7_LEAF:
	case NType::NODE_15_LEAF:
	case NType::NODE_256_LEAF:
		return true;
	default:
		return false;
	}
}

bool NodePointer::IsAnyLeaf() const {
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

void NodePointer::TransformToDeprecated(ART &art, NodePointer &node, TransformToDeprecatedState &state) {
	auto filter = [&](NodePointer current) -> ScanNodeResult {
		auto type = current.GetType();
		if (type == NType::NODE_4 || type == NType::NODE_16 || type == NType::NODE_48 || type == NType::NODE_256) {
			auto &alloc = NodePointer::GetAllocator(art, type);
			if (!alloc.LoadedFromStorage(current)) {
				return ScanNodeResult::SKIP;
			}
		}
		return ScanNodeResult::SCAN_CHILDREN;
	};

	auto pre_handler = [&](NodePointer &child) -> NodePointer {
		D_ASSERT(child.HasMetadata());
		if (child.GetGateStatus() == GateStatus::GATE_SET) {
			Leaf::TransformToDeprecated(art, child);
			return NodePointer();
		}
		auto type = child.GetType();
		switch (type) {
		case NType::PREFIX:
			return PrefixHandle::TransformToDeprecated(art, child, state);
		case NType::LEAF_INLINED:
		case NType::LEAF:
			return NodePointer();
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256:
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
			return child;
		default:
			throw InternalException("invalid node type for TransformToDeprecated: %d", static_cast<int>(type));
		}
	};

	ARTScanPreorder(art, node, filter, pre_handler);
}

//===--------------------------------------------------------------------===//
// Verification
//===--------------------------------------------------------------------===//

void NodePointer::Verify(ART &art) const {
	D_ASSERT(HasMetadata());

	auto type = GetType();
	switch (type) {
	case NType::LEAF_INLINED:
		return;
	case NType::LEAF:
		Leaf::DeprecatedVerify(art, *this);
		return;
	case NType::PREFIX: {
		ConstPrefixHandle::Verify(art, *this);
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

void NodePointer::VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const {
	D_ASSERT(HasMetadata());

	auto filter = [](NodePointer) -> ScanNodeResult {
		return ScanNodeResult::SCAN_CHILDREN;
	};

	auto pre_handler = [&](NodePointer &child) -> NodePointer {
		D_ASSERT(child.HasMetadata());
		auto type = child.GetType();
		switch (type) {
		case NType::LEAF_INLINED:
			return NodePointer();
		case NType::LEAF: {
			auto &leaf = Ref<Leaf>(art, child, type);
			leaf.DeprecatedVerifyAllocations(art, node_counts);
			return NodePointer();
		}
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
			node_counts[GetAllocatorIdx(type)]++;
			return NodePointer();
		case NType::PREFIX:
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256:
			node_counts[GetAllocatorIdx(type)]++;
			return child;
		default:
			throw InternalException("invalid node type for VerifyAllocations: %d", static_cast<int>(type));
		}
	};

	NodePointer root = *this;
	ARTScanPreorder(art, root, filter, pre_handler);
}

//===--------------------------------------------------------------------===//
// Printing
//===--------------------------------------------------------------------===//

namespace {
// Tree-style branch characters
const string NODE_BRANCH_MID = "├── ";
const string NODE_BRANCH_END = "└── ";
const string NODE_VERTICAL = "│   ";
const string NODE_SPACE = "    ";

// ASCII printable character range
constexpr uint8_t NODE_ASCII_PRINTABLE_MIN = 32;
constexpr uint8_t NODE_ASCII_PRINTABLE_MAX = 126;
} // namespace

string NodePointer::ToStringChildren(ART &art, const ToStringOptions &options) const {
	auto format_byte = [&](const uint8_t byte) {
		if (!options.inside_gate && options.display_ascii && byte >= NODE_ASCII_PRINTABLE_MIN &&
		    byte <= NODE_ASCII_PRINTABLE_MAX) {
			return string(1, static_cast<char>(byte));
		}
		return to_string(byte);
	};

	auto is_gate = GetGateStatus() == GateStatus::GATE_SET;
	auto propagate_gate = options.inside_gate || is_gate;
	auto print_full_tree = propagate_gate || !options.key_path || options.expand_after_n_levels == 0;

	string str;

	if (IsLeafNode()) {
		str += options.tree_prefix + NODE_BRANCH_END + "Leaf |";
		uint8_t byte = 0;
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
	} else if (IsNode()) {
		// Collect all children first to know which is last
		vector<pair<uint8_t, const NodePointer *>> children;
		uint8_t byte = 0;
		auto child = GetNextChild(art, byte);
		while (child) {
			children.emplace_back(byte, child.get());
			if (byte == NumericLimits<uint8_t>::Maximum()) {
				break;
			}
			byte++;
			child = GetNextChild(art, byte);
		}

		uint8_t expected_byte = 0;
		auto has_expected_byte = false;
		if (options.key_path && !print_full_tree && options.key_depth < options.key_path->len) {
			expected_byte = (*options.key_path)[options.key_depth];
			has_expected_byte = true;
		}

		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child_entry = children[child_idx];
			auto child_byte = child_entry.first;
			auto &child_ptr = child_entry.second;
			auto is_last = (child_idx == children.size() - 1);
			auto on_path = !has_expected_byte || (has_expected_byte && child_byte == expected_byte);

			// When structure_only is true and we're following a path, treat on-path child as last
			// (since we're hiding off-path siblings, the branch should end here)
			auto effective_last = is_last || (options.structure_only && has_expected_byte && on_path);
			auto branch = effective_last ? NODE_BRANCH_END : NODE_BRANCH_MID;
			auto child_prefix = options.tree_prefix + (effective_last ? NODE_SPACE : NODE_VERTICAL);

			if (on_path) {
				str += options.tree_prefix + branch + format_byte(child_byte) + "\n";

				auto child_options = options;
				child_options.inside_gate = propagate_gate;
				child_options.key_depth = has_expected_byte ? options.key_depth + 1 : options.key_depth;
				child_options.expand_after_n_levels =
				    (options.expand_after_n_levels > 0) ? options.expand_after_n_levels - 1 : 0;

				auto child_type = child_ptr->GetType();
				auto is_internal = (child_type == NType::NODE_4 || child_type == NType::NODE_16 ||
				                    child_type == NType::NODE_48 || child_type == NType::NODE_256);
				if (is_internal) {
					str += child_prefix + NODE_BRANCH_END + "NodePointer" + to_string(GetCapacity(child_type)) + "\n";
					child_options.tree_prefix = child_prefix + NODE_SPACE;
					str += child_ptr->ToStringChildren(art, child_options);
				} else {
					child_options.tree_prefix = child_prefix;
					str += child_ptr->ToString(art, child_options);
				}
			} else {
				if (!options.structure_only) {
					str += options.tree_prefix + branch + format_byte(child_byte) + " ...\n";
				}
			}
		}
	}
	return str;
}

string NodePointer::ToString(ART &art, const ToStringOptions &options) const {
	auto type = GetType();
	auto is_gate = GetGateStatus() == GateStatus::GATE_SET;
	auto propagate_gate = options.inside_gate || is_gate;

	switch (type) {
	case NType::LEAF_INLINED: {
		return options.tree_prefix + "Inlined Leaf [row ID: " + to_string(GetRowId()) + "]\n";
	}
	case NType::LEAF: {
		return Leaf::DeprecatedToString(art, *this, options);
	}
	case NType::PREFIX: {
		auto prefix_options = options;
		prefix_options.inside_gate = propagate_gate;
		auto str = ConstPrefixHandle::ToString(art, *this, prefix_options);
		if (is_gate) {
			str = options.tree_prefix + "Gate\n" + str;
		}
		return str;
	}
	default:
		break;
	}

	// For internal nodes: print header then children
	auto str = options.tree_prefix + "NodePointer" + to_string(GetCapacity(type)) + "\n";
	str += ToStringChildren(art, options);

	if (is_gate && type != NType::PREFIX) {
		str = options.tree_prefix + "Gate\n" + str;
	}
	return str;
}

} // namespace duckdb
