//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/const_prefix_handle.hpp"
#include "duckdb/execution/index/art/node_handle.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/prefix_handle.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/base_node.hpp"

namespace duckdb {

//! ARTOperator provides functionality for different ART operations.
class ARTOperator {
public:
	//! Lookup returns the leaf matching the key, or an empty OptionalNodePtr if no such leaf exists.
	static OptionalNodePtr Lookup(const ART &art, const NodePtr &node_ptr, const ARTKey &key, idx_t depth) {
		NodePtr current_ptr(node_ptr);

		while (current_ptr.HasMetadata()) {
			if (current_ptr.IsAnyLeaf() || current_ptr.GetGateStatus() == GateStatus::GATE_SET) {
				return current_ptr;
			}

			if (current_ptr.GetType() == NType::PREFIX) {
				ConstNodeHandle handle(art, current_ptr);
				auto data = handle.GetPtr();
				NodePtr child_ptr = ConstPrefixHandle::ChildRef(art, handle);
				for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
					if (data[i] != key[depth]) {
						return OptionalNodePtr();
					}
					depth++;
				}
				current_ptr = child_ptr;
				continue;
			}

			D_ASSERT(depth < key.len);
			auto child_ptr = current_ptr.GetChildNode(art, key[depth]);
			if (!child_ptr) {
				return OptionalNodePtr();
			}

			current_ptr = child_ptr.Get();
			D_ASSERT(current_ptr.HasMetadata());
			depth++;
		}

		return OptionalNodePtr();
	}

	//! LookupInLeaf returns true if the rowid is in the leaf:
	//! 1) If the leaf is an inlined leaf, check if the rowid matches.
	//! 2) If the leaf is a gate node, perform a search in the nested ART for the rowid.
	static bool LookupInLeaf(const ART &art, const NodePtr &node_ptr, const ARTKey &rowid) {
		NodePtr current_ptr(node_ptr);
		idx_t depth = 0;

		while (current_ptr.HasMetadata()) {
			const auto type = current_ptr.GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				return current_ptr.GetRowId() == rowid.GetRowId();
			}
			case NType::LEAF: {
				throw InternalException("Invalid node type (LEAF) for ARTOperator::LookupInLeaf.");
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				D_ASSERT(depth + 1 == Prefix::ROW_ID_SIZE);
				const auto byte = rowid[Prefix::ROW_ID_COUNT];
				return current_ptr.HasByte(art, byte);
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				D_ASSERT(depth < Prefix::ROW_ID_SIZE);
				auto child_ptr = current_ptr.GetChildNode(art, rowid[depth]);
				if (child_ptr) {
					// Continue in the child.
					current_ptr = child_ptr.Get();
					depth++;
					D_ASSERT(current_ptr.HasMetadata());
					continue;
				}
				return false;
			}
			case NType::PREFIX: {
				ConstNodeHandle handle(art, current_ptr);
				auto data = handle.GetPtr();
				NodePtr child_ptr = ConstPrefixHandle::ChildRef(art, handle);
				for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
					if (data[i] != rowid[depth]) {
						// The key and the prefix don't match.
						return false;
					}
					depth++;
				}
				current_ptr = child_ptr;
				break;
			}
			default:
				throw InternalException("Invalid node type for ARTOperator::LookupInLeaf: %d.", type);
			}
		}
		return false;
	}

	//! Insert a key and its row ID into the node.
	//! Starts at depth (in the key).
	//! status indicates if the insert happens inside a gate or not.
	//! The caller must keep the storage containing node_ptr valid for the duration of the call.
	static ARTConflictType Insert(ArenaAllocator &arena, ART &art, NodePtr &node_ptr, const ARTKey &key, idx_t depth,
	                              const ARTKey &row_id, GateStatus status, DeleteIndexInfo delete_index_info,
	                              const IndexAppendMode append_mode) {
		// Keep the containing node pinned while active_node_ptr_ref refers to its child pointer.
		optional<NodeHandle> parent_node_handle;
		reference<NodePtr> active_node_ptr_ref(node_ptr);
		reference<const ARTKey> active_key_ref(key);

		// Early-out, if the node is empty.
		if (!node_ptr.HasMetadata()) {
			D_ASSERT(depth == 0);
			if (status == GateStatus::GATE_SET) {
				Leaf::New(node_ptr, row_id.GetRowId());
				return ARTConflictType::NO_CONFLICT;
			}

			// Empty keys (NULLs) must be filtered by callers.
			D_ASSERT(key.len != 0);
			auto chain = PrefixHandle::New(art, key, depth, key.len);
			Leaf::New(chain.tail.Child(art), row_id.GetRowId());
			node_ptr = chain.root_ptr;
			return ARTConflictType::NO_CONFLICT;
		}

		while (true) {
			auto &active_node_ptr = active_node_ptr_ref.get();
			if (!active_node_ptr.HasMetadata()) {
				throw InternalException("node without metadata in ARTOperator::Insert");
			}
			auto &active_key = active_key_ref.get();

			// status is GATE_SET, if we've passed a gate in the previous iteration.
			// In that case, we have not adjusted the reference.
			if (status == GateStatus::GATE_NOT_SET && active_node_ptr.GetGateStatus() == GateStatus::GATE_SET) {
				if (!art.IsUnique()) {
					// Enter a gate.
					active_key_ref = row_id;
					depth = 0;
					status = GateStatus::GATE_SET;
					continue;
				}
				// A key in a unique ART has a single row ID. The exception is a commit that deleted the key's
				// committed row and appends the key again: the new row ID sits next to the deleted one (a gate)
				// until the commit removes the deleted row ID. Commit-time appends are serialized, so this gate
				// was created by the current commit, and this insert appends the same key a second time, e.g.,
				// for a deferred constraint. That is a constraint violation, provided that the gate holds the row
				// this commit deleted. Otherwise, the index is corrupted.
				if (GateContainsDeletedRow(arena, art, active_node_ptr, key, delete_index_info)) {
					return ARTConflictType::CONSTRAINT;
				}
				throw FatalException("Corrupted unique ART index %s: encountered an existing gated leaf in unique "
				                     "index while inserting",
				                     art.name);
			}

			const auto type = active_node_ptr.GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				return InsertIntoInlined(arena, art, active_node_ptr, key, row_id, depth, status, delete_index_info,
				                         append_mode);
			}
			case NType::LEAF: {
				Leaf::TransformToNested(art, active_node_ptr);
				continue;
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				// Row IDs are unique; there are never any duplicate byte conflicts.
				auto byte = active_key[Prefix::ROW_ID_COUNT];
				NodePtr::InsertChild(art, active_node_ptr, byte);
				return ARTConflictType::NO_CONFLICT;
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				D_ASSERT(depth < active_key.len);
				{
					NodeHandle node_handle(art, active_node_ptr);
					auto child_ptr_ref = NodePtr::GetChildMutable(node_handle, active_key[depth]);
					if (child_ptr_ref) {
						active_node_ptr_ref = *child_ptr_ref;
						parent_node_handle = std::move(node_handle);
						depth++;
						D_ASSERT(active_node_ptr_ref.get().HasMetadata());
						continue;
					}
				}
				InsertIntoNode(art, active_node_ptr, key, row_id, depth, status);
				return ARTConflictType::NO_CONFLICT;
			}
			case NType::PREFIX: {
				idx_t pos = 0;
				uint8_t count;
				uint8_t byte = 0;
				{
					NodeHandle prefix_handle(art, active_node_ptr);
					auto data = prefix_handle.GetPtr();
					count = data[art.PrefixCount()];
					for (; pos < count; pos++) {
						D_ASSERT(depth < active_key.len);
						byte = data[pos];
						if (byte != active_key[depth]) {
							break;
						}
						depth++;
					}

					if (pos == count) {
						active_node_ptr_ref = PrefixHandle::ChildRef(art, prefix_handle);
						parent_node_handle = std::move(prefix_handle);
					}
				}
				if (pos != count) {
					// The active key and the prefix don't match.
					InsertIntoPrefix(art, active_node_ptr, active_key, row_id, pos, byte, depth, status);
					return ARTConflictType::NO_CONFLICT;
				}
				D_ASSERT(active_node_ptr_ref.get().HasMetadata());
				continue;
			}
			default:
				throw InternalException("Invalid node type for ARTOperator::Insert.");
			}
		}
	}

	//! Delete a key and its row ID.
	//! Assumes that deletion starts at the root of the tree.
	//! The caller must keep the storage containing node_ptr valid for the duration of the call.
	static bool Delete(ART &art, NodePtr &node_ptr, const ARTKey &key, const ARTKey &row_id) {
		// If we need to compress a Node4 into a one-way node,
		// then we need the previous prefix before the Node4.
		// The four handles form a fixed-size window: pins never accumulate with the depth of the descent.
		// parent and current start at the root, which no node stores, so their handles are unpinned.
		// At a root Node4, DeleteChild thus receives the Node4's own NodePtr as parent_ptr.
		// empty_ptr is never read: that would require the root to be an inlined leaf.
		NodePtr empty_ptr;
		auto great_grandparent = NodePtrHandle::Unpinned(empty_ptr);
		auto grandparent = NodePtrHandle::Unpinned(empty_ptr);
		auto parent = NodePtrHandle::Unpinned(node_ptr);
		auto current = NodePtrHandle::Unpinned(node_ptr);
		reference<const ARTKey> current_key_ref(key);

		idx_t grandparent_depth = 0;
		idx_t parent_depth = 0;
		idx_t depth = 0;
		auto status = GateStatus::GATE_NOT_SET;
		auto passed_internal_node = false;

		// Moves the window one level down: each level takes the handle of the level below it, so current becomes the
		// parent and the great-grandparent's pin is dropped. current is left empty; the caller sets it to a child.
		auto move_window_down = [&]() {
			great_grandparent = std::move(grandparent);
			grandparent = std::move(parent);
			parent = std::move(current);
			grandparent_depth = parent_depth;
			parent_depth = depth;
		};

		while (current.Get().HasMetadata()) {
			// Enter gate.
			if (status == GateStatus::GATE_NOT_SET && current.Get().GetGateStatus() == GateStatus::GATE_SET) {
				status = GateStatus::GATE_SET;
				current_key_ref = row_id;
				depth = 0;
				continue;
			}

			const auto type = current.Get().GetType();
			switch (type) {
			case NType::LEAF_INLINED: {
				if (current.Get().GetRowId() != row_id.GetRowId()) {
					return false;
				}
				if (!passed_internal_node && parent.Get().GetType() == NType::PREFIX) {
					// The tree contains exactly one element with a prefix.
					// The freed chain stores the current NodePtr: release its pin first.
					current.Release();
					NodePtr::FreeTree(art, parent.Get());
					return true;
				}
				if (parent.Get().GetType() == NType::PREFIX) {
					// We might have to compress:
					// PREFIX (greatgrandparent) - Node4 (grandparent) - PREFIX - INLINED_LEAF.
					// The parent does not have to be passed in, as it is a child of the possibly being compressed N4.
					// Then, when we delete that child, we also free it.
					// DeleteChild frees the parent chain, and may free the grandparent node by compressing or
					// shrinking it: release their pins first.
					current.Release();
					parent.Release();
					NodePtr::DeleteChild(art, std::move(grandparent), great_grandparent.Get(),
					                     current_key_ref.get()[grandparent_depth], status, row_id);
					return true;
				}
				// Compressing or shrinking the parent node frees the node pinned by current: release it first.
				current.Release();
				NodePtr::DeleteChild(art, std::move(parent), grandparent.Get(), current_key_ref.get()[parent_depth],
				                     status, row_id);
				return true;
			}
			case NType::LEAF: {
				D_ASSERT(status == GateStatus::GATE_NOT_SET);
				Leaf::TransformToNested(art, current.Get());
				break;
			}
			case NType::PREFIX: {
				move_window_down();

				// Traverse a prefix chain until the next non-prefix node or gate,
				// releasing each prefix node's pin once the next one is pinned.
				NodePtr prefix_ptr = parent.Get();
				while (prefix_ptr.GetType() == NType::PREFIX) {
					NodeHandle prefix_handle(art, prefix_ptr);
					auto data = prefix_handle.GetPtr();
					for (idx_t i = 0; i < data[art.PrefixCount()]; i++) {
						if (data[i] != current_key_ref.get()[depth]) {
							return false;
						}
						depth++;
					}
					current = NodePtrHandle::PrefixChild(art, std::move(prefix_handle));
					prefix_ptr = current.Get();
					if (prefix_ptr.GetGateStatus() == GateStatus::GATE_SET) {
						break;
					}
				}
				break;
			}
			case NType::NODE_4:
			case NType::NODE_16:
			case NType::NODE_48:
			case NType::NODE_256: {
				passed_internal_node = true;
				move_window_down();

				auto child_ptr_handle =
				    NodePtrHandle::Child(NodeHandle(art, parent.Get()), current_key_ref.get()[depth]);
				if (!child_ptr_handle) {
					// No child at the byte: nothing to erase.
					return false;
				}

				current = std::move(*child_ptr_handle);
				depth++;
				break;
			}
			case NType::NODE_7_LEAF:
			case NType::NODE_15_LEAF:
			case NType::NODE_256_LEAF: {
				const auto byte = current_key_ref.get()[depth];
				if (current.Get().HasByte(art, byte)) {
					NodePtr::DeleteChild(art, std::move(current), parent.Get(), byte, status, row_id);
					return true;
				}
				return false;
			}
			}
		}
		return false;
	}

private:
	//! Returns true, if the gate contains the row ID that a delete index stores for the key.
	static bool GateContainsDeletedRow(ArenaAllocator &arena, const ART &art, const NodePtr &gate_ptr,
	                                   const ARTKey &key, DeleteIndexInfo delete_index_info) {
		if (!delete_index_info.delete_indexes) {
			return false;
		}
		for (auto &delete_index : *delete_index_info.delete_indexes) {
			auto &delete_art = delete_index.get().Cast<ART>();
			auto delete_leaf_ptr = Lookup(delete_art, delete_art.root_ptr, key, 0);
			if (!delete_leaf_ptr || delete_leaf_ptr.Get().GetType() != NType::LEAF_INLINED) {
				continue;
			}
			auto deleted_row_id = ARTKey::CreateARTKey<row_t>(arena, delete_leaf_ptr.Get().GetRowId());
			if (LookupInLeaf(art, gate_ptr, deleted_row_id)) {
				return true;
			}
		}
		return false;
	}

	static ARTConflictType InsertIntoInlined(ArenaAllocator &arena, ART &art, NodePtr &node_ptr, const ARTKey &key,
	                                         const ARTKey &row_id, const idx_t depth, const GateStatus status,
	                                         DeleteIndexInfo delete_index_info, const IndexAppendMode append_mode) {
		NodePtr row_id_node_ptr;
		Leaf::New(row_id_node_ptr, row_id.GetRowId());

		if (!art.IsUnique() || append_mode == IndexAppendMode::INSERT_DUPLICATES) {
			Leaf::MergeInlined(arena, art, node_ptr, row_id_node_ptr, status, depth);
			return ARTConflictType::NO_CONFLICT;
		}

		if (delete_index_info.delete_indexes) {
			// Lookup in the delete_art.
			for (auto &delete_index : *delete_index_info.delete_indexes) {
				auto &delete_art = delete_index.get().Cast<ART>();
				auto delete_leaf_ptr = Lookup(delete_art, delete_art.root_ptr, key, 0);
				if (!delete_leaf_ptr) {
					continue;
				}

				// The row ID has changed.
				// Thus, the local index has a newer (local) row ID, and this is a constraint violation.
				D_ASSERT(delete_leaf_ptr.Get().GetType() == NType::LEAF_INLINED);
				auto deleted_row_id = delete_leaf_ptr.Get().GetRowId();
				auto this_row_id = node_ptr.GetRowId();
				if (deleted_row_id != this_row_id) {
					continue;
				}

				// The deleted key and its row ID match the current key and its row ID.
				Leaf::MergeInlined(arena, art, node_ptr, row_id_node_ptr, status, depth);
				return ARTConflictType::NO_CONFLICT;
			}
		}
		if (append_mode == IndexAppendMode::IGNORE_DUPLICATES) {
			return ARTConflictType::NO_CONFLICT;
		}
		return ARTConflictType::CONSTRAINT;
	}

	static void InsertIntoNode(ART &art, NodePtr &node_ptr, const ARTKey &key, const ARTKey &row_id, const idx_t depth,
	                           const GateStatus status) {
		if (status == GateStatus::GATE_SET) {
			// Inside gates, we compress prefixes that only have an inlined
			// row ID as their child.
			NodePtr row_id_node_ptr;
			Leaf::New(row_id_node_ptr, row_id.GetRowId());
			NodePtr::InsertChild(art, node_ptr, row_id[depth], row_id_node_ptr);
			return;
		}

		NodePtr leaf_ptr;
		if (depth + 1 < key.len) {
			// Outside of gates, we create a prefix for the inlined leaf.
			auto count = key.len - depth - 1;
			auto chain = PrefixHandle::New(art, key, depth + 1, count);
			Leaf::New(chain.tail.Child(art), row_id.GetRowId());
			leaf_ptr = chain.root_ptr;
		} else {
			Leaf::New(leaf_ptr, row_id.GetRowId());
		}

		NodePtr::InsertChild(art, node_ptr, key[depth], leaf_ptr);
	}

	static void InsertIntoPrefix(ART &art, NodePtr &node_ptr, const ARTKey &key, const ARTKey &row_id, const idx_t pos,
	                             const uint8_t byte, const idx_t depth, const GateStatus status) {
		const auto cast_pos = UnsafeNumericCast<uint8_t>(pos);

		NodePtr branching_node4_ptr;
		Node4::New(art, branching_node4_ptr);
		auto child_ptr = PrefixHandle::Split(art, node_ptr, branching_node4_ptr, cast_pos);

		Node4::InsertChild(art, branching_node4_ptr, byte, child_ptr);
		InsertIntoNode(art, branching_node4_ptr, key, row_id, depth, status);
	}
};

} // namespace duckdb
