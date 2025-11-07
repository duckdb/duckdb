//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"

namespace duckdb {

//! ARTOperator provides functionality for different ART operations.
class ARTOperator {
public:
	//! Lookup returns a pointer to the leaf matching the key,
	//! or nullptr, if no such leaf exists.
	static unsafe_optional_ptr<const Node> Lookup(ART &art, const Node &node, const ARTKey &key, idx_t depth) {
		reference<const Node> ref(node);

		while (ref.get().HasMetadata()) {
			// Return the leaf.
			if (ref.get().IsAnyLeaf() || ref.get().GetGateStatus() == GateStatus::GATE_SET) {
				return unsafe_optional_ptr<const Node>(ref.get());
			}

			// Traverse the prefix.
			if (ref.get().GetType() == NType::PREFIX) {
				Prefix prefix(art, ref.get());
				for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
					if (prefix.data[i] != key[depth]) {
						// The key and the prefix don't match.
						return nullptr;
					}
					depth++;
				}
				ref = *prefix.ptr;
				continue;
			}

			// Get the child node.
			D_ASSERT(depth < key.len);
			auto child = ref.get().GetChild(art, key[depth]);

			// No child at the key byte, return nullptr.
			if (!child) {
				return nullptr;
			}

			// Continue in the child.
			ref = *child;
			D_ASSERT(ref.get().HasMetadata());
			depth++;
		}

		return nullptr;
	}

	//! LookupInLeaf returns true if the rowid is in the leaf:
	//! 1) If the leaf is an inlined leaf, check if the rowid matches.
	//! 2) If the leaf is a gate node, perform a search in the nested ART for the rowid.
	static bool LookupInLeaf(ART &art, const Node &node, const ARTKey &rowid) {
		reference<const Node> ref(node);
		idx_t depth = 0;

		while (ref.get().HasMetadata()) {
			const auto type = ref.get().GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				return ref.get().GetRowId() == rowid.GetRowId();
			}
			case NType::LEAF: {
				throw InternalException("Invalid node type (LEAF) for ARTOperator::NestedLookup.");
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				D_ASSERT(depth + 1 == Prefix::ROW_ID_SIZE);
				const auto byte = rowid[Prefix::ROW_ID_COUNT];
				return ref.get().HasByte(art, byte);
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				D_ASSERT(depth < Prefix::ROW_ID_SIZE);
				auto child = ref.get().GetChild(art, rowid[depth]);
				if (child) {
					// Continue in the child.
					ref = *child;
					depth++;
					D_ASSERT(ref.get().HasMetadata());
					continue;
				}
				return false;
			}
			case NType::PREFIX: {
				Prefix prefix(art, ref.get());
				for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
					if (prefix.data[i] != rowid[depth]) {
						// The key and the prefix don't match.
						return false;
					}
					depth++;
				}
				ref = *prefix.ptr;
			}
			}
		}
		return false;
	}

	//! Insert a key and its row ID into the node.
	//! Starts at depth (in the key).
	//! status indicates if the insert happens inside a gate or not.
	static ARTConflictType Insert(ArenaAllocator &arena, ART &art, Node &node, const ARTKey &key, idx_t depth,
	                              const ARTKey &row_id, GateStatus status, optional_ptr<ART> delete_art,
	                              const IndexAppendMode append_mode) {
		reference<Node> active_node_ref(node);
		reference<const ARTKey> active_key_ref(key);

		// Early-out, if the node is empty.
		if (!node.HasMetadata()) {
			D_ASSERT(depth == 0);
			if (status == GateStatus::GATE_SET) {
				Leaf::New(node, row_id.GetRowId());
				return ARTConflictType::NO_CONFLICT;
			}

			Prefix::New(art, active_node_ref, active_key_ref.get(), depth, active_key_ref.get().len);
			Leaf::New(active_node_ref, row_id.GetRowId());
			return ARTConflictType::NO_CONFLICT;
		}

		while (active_node_ref.get().HasMetadata()) {
			auto &active_node = active_node_ref.get();
			auto &active_key = active_key_ref.get();

			// status is GATE_SET, if we've passed a gate in the previous iteration.
			// In that case, we have not adjusted the reference.
			if (status == GateStatus::GATE_NOT_SET && active_node.GetGateStatus() == GateStatus::GATE_SET) {
				if (!art.IsUnique()) {
					// Enter a gate.
					active_key_ref = row_id;
					depth = 0;
					status = GateStatus::GATE_SET;
					continue;
				}
				// Unique indexes can have duplicates, if another transaction DELETE + INSERT
				// the same key. In that case, the previous value must be kept alive until all
				// other transactions do not depend on it anymore.

				// We restrict this transactionality to two-value leaves, so any subsequent
				// incoming transaction must fail here.
				return ARTConflictType::TRANSACTION;
			}

			const auto type = active_node.GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				return InsertIntoInlined(arena, art, active_node, key, row_id, depth, status, delete_art, append_mode);
			}
			case NType::LEAF: {
				Leaf::TransformToNested(art, active_node);
				continue;
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				// Row IDs are unique; there are never any duplicate byte conflicts.
				auto byte = active_key[Prefix::ROW_ID_COUNT];
				Node::InsertChild(art, active_node, byte);
				return ARTConflictType::NO_CONFLICT;
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				D_ASSERT(depth < active_key.len);
				auto child = active_node.GetChildMutable(art, active_key[depth]);
				if (child) {
					// Continue in the child.
					active_node_ref = *child;
					depth++;
					D_ASSERT(active_node_ref.get().HasMetadata());
					continue;
				}
				InsertIntoNode(art, active_node, key, row_id, depth, status);
				return ARTConflictType::NO_CONFLICT;
			}
			case NType::PREFIX: {
				Prefix prefix(art, active_node, true);
				for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
					if (prefix.data[i] != active_key[depth]) {
						// The active key and the prefix don't match.
						InsertIntoPrefix(art, active_node_ref, active_key, row_id, i, depth, status);
						return ARTConflictType::NO_CONFLICT;
					}
					depth++;
				}
				active_node_ref = *prefix.ptr;
				D_ASSERT(active_node_ref.get().HasMetadata());
				continue;
			}
			default:
				throw InternalException("Invalid node type for ARTOperator::Insert.");
			}
		}
		throw InternalException("node without metadata in ARTOperator::Insert");
	}

	//! Delete a key and its row ID.
	//! Assumes that deletion starts at the root of the tree.
	static void Delete(ART &art, Node &node, const ARTKey &key, const ARTKey &row_id) {
		// If we need to compress a Node4 into a one-way node,
		// then we need the previous prefix before the Node4.
		Node empty;
		reference<Node> greatgrandparent(empty);
		reference<Node> grandparent(empty);
		reference<Node> parent(node);
		reference<Node> current(node);
		reference<const ARTKey> current_key(key);

		idx_t grandparent_depth = 0;
		idx_t parent_depth = 0;
		idx_t depth = 0;
		auto status = GateStatus::GATE_NOT_SET;
		auto passed_node = false;

		while (current.get().HasMetadata()) {
			// Enter gate.
			if (status == GateStatus::GATE_NOT_SET && current.get().GetGateStatus() == GateStatus::GATE_SET) {
				status = GateStatus::GATE_SET;
				current_key = row_id;
				depth = 0;
				continue;
			}

			const auto type = current.get().GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				if (current.get().GetRowId() != row_id.GetRowId()) {
					return;
				}
				if (!passed_node && parent.get().GetType() == NType::PREFIX) {
					// The tree contains exactly one element with a prefix.
					Node::FreeTree(art, parent);
					return;
				}
				if (parent.get().GetType() == NType::PREFIX) {
					// We might have to compress:
					// PREFIX (greatgrandparent) - Node4 (grandparent) - PREFIX - INLINED_LEAF.
					Node::DeleteChild(art, grandparent, greatgrandparent, current_key.get()[grandparent_depth], status,
					                  row_id);
					return;
				}
				Node::DeleteChild(art, parent, grandparent, current_key.get()[parent_depth], status, row_id);
				return;
			}
			case NType::LEAF: {
				D_ASSERT(status == GateStatus::GATE_NOT_SET);
				Leaf::TransformToNested(art, current);
				break;
			}
			case NType::PREFIX: {
				greatgrandparent = grandparent;
				grandparent = parent;
				parent = current;
				grandparent_depth = parent_depth;
				parent_depth = depth;

				// Traverse a prefix chain until the next non-prefix node or gate.
				while (current.get().GetType() == NType::PREFIX) {
					Prefix prefix(art, current, true);
					for (idx_t i = 0; i < prefix.data[Prefix::Count(art)]; i++) {
						if (prefix.data[i] != current_key.get()[depth]) {
							return;
						}
						depth++;
					}
					current = *prefix.ptr;
					if (current.get().GetGateStatus() == GateStatus::GATE_SET) {
						break;
					}
				}
				break;
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				passed_node = true;
				greatgrandparent = grandparent;
				grandparent = parent;
				parent = current;
				grandparent_depth = parent_depth;
				parent_depth = depth;

				auto child = current.get().GetChildMutable(art, current_key.get()[depth]);
				if (!child) {
					// No child at the byte: nothing to erase.
					return;
				}

				current = *child;
				depth++;
				break;
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				const auto byte = current_key.get()[depth];
				if (current.get().HasByte(art, byte)) {
					Node::DeleteChild(art, current, parent, byte, status, row_id);
				}
				return;
			}
			}
		}
	}

private:
	static ARTConflictType InsertIntoInlined(ArenaAllocator &arena, ART &art, Node &node, const ARTKey &key,
	                                         const ARTKey &row_id, const idx_t depth, const GateStatus status,
	                                         optional_ptr<ART> delete_art, const IndexAppendMode append_mode) {
		Node row_id_node;
		Leaf::New(row_id_node, row_id.GetRowId());

		if (!art.IsUnique() || append_mode == IndexAppendMode::INSERT_DUPLICATES) {
			Leaf::MergeInlined(arena, art, node, row_id_node, status, depth);
			return ARTConflictType::NO_CONFLICT;
		}

		if (!delete_art) {
			if (append_mode == IndexAppendMode::IGNORE_DUPLICATES) {
				return ARTConflictType::NO_CONFLICT;
			}
			return ARTConflictType::CONSTRAINT;
		}

		// Lookup in the delete_art.
		auto delete_leaf = Lookup(*delete_art, delete_art->tree, key, 0);
		if (!delete_leaf) {
			return ARTConflictType::CONSTRAINT;
		}

		// The row ID has changed.
		// Thus, the local index has a newer (local) row ID, and this is a constraint violation.
		D_ASSERT(delete_leaf->GetType() == NType::LEAF_INLINED);
		auto deleted_row_id = delete_leaf->GetRowId();
		auto this_row_id = node.GetRowId();
		if (deleted_row_id != this_row_id) {
			return ARTConflictType::CONSTRAINT;
		}

		// The deleted key and its row ID match the current key and its row ID.
		Leaf::MergeInlined(arena, art, node, row_id_node, status, depth);
		return ARTConflictType::NO_CONFLICT;
	}

	static void InsertIntoNode(ART &art, Node &node, const ARTKey &key, const ARTKey &row_id, const idx_t depth,
	                           const GateStatus status) {
		if (status == GateStatus::GATE_SET) {
			// Inside gates, we compress prefixes that only have an inlined
			// row ID as their child.
			Node row_id_node;
			Leaf::New(row_id_node, row_id.GetRowId());
			Node::InsertChild(art, node, row_id[depth], row_id_node);
			return;
		}

		Node leaf;
		reference<Node> leaf_ref(leaf);
		if (depth + 1 < key.len) {
			// Outside of gates, we create a prefix for the inlined leaf.
			auto count = key.len - depth - 1;
			Prefix::New(art, leaf_ref, key, depth + 1, count);
		}

		// Create and insert the inlined leaf.
		Leaf::New(leaf_ref, row_id.GetRowId());
		Node::InsertChild(art, node, key[depth], leaf);
	}

	static void InsertIntoPrefix(ART &art, reference<Node> &node_ref, const ARTKey &key, const ARTKey &row_id,
	                             const idx_t pos, const idx_t depth, const GateStatus status) {
		const auto cast_pos = UnsafeNumericCast<uint8_t>(pos);
		const auto byte = Prefix::GetByte(art, node_ref, cast_pos);

		Node child;
		const auto split_status = Prefix::Split(art, node_ref, child, cast_pos);

		Node4::New(art, node_ref);
		node_ref.get().SetGateStatus(split_status);

		Node4::InsertChild(art, node_ref, byte, child);
		InsertIntoNode(art, node_ref, key, row_id, depth, status);
	}
};

} // namespace duckdb
