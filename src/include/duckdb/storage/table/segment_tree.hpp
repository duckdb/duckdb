//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/segment_lock.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

template <class T>
struct SegmentNode {
	idx_t row_start;
	unique_ptr<T> node;
};

//! The SegmentTree maintains a list of all segments of a specific column in a table, and allows searching for a segment
//! by row number
template <class T, bool SUPPORTS_LAZY_LOADING = false>
class SegmentTree {
private:
	class SegmentIterationHelper;

public:
	explicit SegmentTree() : finished_loading(true) {
	}
	virtual ~SegmentTree() {
	}

	//! Locks the segment tree. All methods to the segment tree either lock the segment tree, or take an already
	//! obtained lock.
	SegmentLock Lock() {
		return SegmentLock(node_lock);
	}

	bool IsEmpty(SegmentLock &l) {
		return GetRootSegment(l) == nullptr;
	}

	//! Gets a pointer to the first segment. Useful for scans.
	T *GetRootSegment() {
		auto l = Lock();
		return GetRootSegment(l);
	}

	T *GetRootSegment(SegmentLock &l) {
		if (nodes.empty()) {
			LoadNextSegment(l);
		}
		return GetRootSegmentInternal();
	}
	//! Obtains ownership of the data of the segment tree
	vector<SegmentNode<T>> MoveSegments(SegmentLock &l) {
		LoadAllSegments(l);
		return std::move(nodes);
	}
	vector<SegmentNode<T>> MoveSegments() {
		auto l = Lock();
		return MoveSegments(l);
	}
	idx_t GetSegmentCount() {
		auto l = Lock();
		return GetSegmentCount(l);
	}
	idx_t GetSegmentCount(SegmentLock &l) {
		return nodes.size();
	}
	//! Gets a pointer to the nth segment. Negative numbers start from the back.
	T *GetSegmentByIndex(int64_t index) {
		auto l = Lock();
		return GetSegmentByIndex(l, index);
	}
	T *GetSegmentByIndex(SegmentLock &l, int64_t index) {
		if (index < 0) {
			// load all segments
			LoadAllSegments(l);
			index += nodes.size();
			if (index < 0) {
				return nullptr;
			}
			return nodes[UnsafeNumericCast<idx_t>(index)].node.get();
		} else {
			// lazily load segments until we reach the specific segment
			while (idx_t(index) >= nodes.size() && LoadNextSegment(l)) {
			}
			if (idx_t(index) >= nodes.size()) {
				return nullptr;
			}
			return nodes[UnsafeNumericCast<idx_t>(index)].node.get();
		}
	}
	//! Gets the next segment
	T *GetNextSegment(T *segment) {
		if (!SUPPORTS_LAZY_LOADING) {
			return segment->Next();
		}
		if (finished_loading) {
			return segment->Next();
		}
		auto l = Lock();
		return GetNextSegment(l, segment);
	}
	T *GetNextSegment(SegmentLock &l, T *segment) {
		if (!segment) {
			return nullptr;
		}
#ifdef DEBUG
		D_ASSERT(nodes[segment->index].node.get() == segment);
#endif
		return GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(segment->index + 1));
	}

	//! Gets a pointer to the last segment. Useful for appends.
	T *GetLastSegment(SegmentLock &l) {
		LoadAllSegments(l);
		if (nodes.empty()) {
			return nullptr;
		}
		return nodes.back().node.get();
	}
	//! Gets a pointer to a specific column segment for the given row
	T *GetSegment(idx_t row_number) {
		auto l = Lock();
		return GetSegment(l, row_number);
	}
	T *GetSegment(SegmentLock &l, idx_t row_number) {
		return nodes[GetSegmentIndex(l, row_number)].node.get();
	}

	//! Append a column segment to the tree
	void AppendSegmentInternal(SegmentLock &l, unique_ptr<T> segment) {
		D_ASSERT(segment);
		// add the node to the list of nodes
		if (!nodes.empty()) {
			nodes.back().node->next = segment.get();
		}
		SegmentNode<T> node;
		segment->index = nodes.size();
		node.row_start = segment->start;
		node.node = std::move(segment);
		nodes.push_back(std::move(node));
	}
	void AppendSegment(unique_ptr<T> segment) {
		auto l = Lock();
		AppendSegment(l, std::move(segment));
	}
	void AppendSegment(SegmentLock &l, unique_ptr<T> segment) {
		LoadAllSegments(l);
		AppendSegmentInternal(l, std::move(segment));
	}
	//! Debug method, check whether the segment is in the segment tree
	bool HasSegment(T *segment) {
		auto l = Lock();
		return HasSegment(l, segment);
	}
	bool HasSegment(SegmentLock &, T *segment) {
		return segment->index < nodes.size() && nodes[segment->index].node.get() == segment;
	}

	//! Replace this tree with another tree, taking over its nodes in-place
	void Replace(SegmentTree<T> &other) {
		auto l = Lock();
		Replace(l, other);
	}
	void Replace(SegmentLock &l, SegmentTree<T> &other) {
		other.LoadAllSegments(l);
		nodes = std::move(other.nodes);
	}

	//! Erase all segments after a specific segment
	void EraseSegments(SegmentLock &l, idx_t segment_start) {
		LoadAllSegments(l);
		if (segment_start >= nodes.size() - 1) {
			return;
		}
		nodes.erase(nodes.begin() + UnsafeNumericCast<int64_t>(segment_start) + 1, nodes.end());
	}

	//! Get the segment index of the column segment for the given row
	idx_t GetSegmentIndex(SegmentLock &l, idx_t row_number) {
		idx_t segment_index;
		if (TryGetSegmentIndex(l, row_number, segment_index)) {
			return segment_index;
		}
		string error;
		error = StringUtil::Format("Attempting to find row number \"%lld\" in %lld nodes\n", row_number, nodes.size());
		for (idx_t i = 0; i < nodes.size(); i++) {
			error += StringUtil::Format("Node %lld: Start %lld, Count %lld", i, nodes[i].row_start,
			                            nodes[i].node->count.load());
		}
		throw InternalException("Could not find node in column segment tree!\n%s%s", error, Exception::GetStackTrace());
	}

	bool TryGetSegmentIndex(SegmentLock &l, idx_t row_number, idx_t &result) {
		// load segments until the row number is within bounds
		while (nodes.empty() || (row_number >= (nodes.back().row_start + nodes.back().node->count))) {
			if (!LoadNextSegment(l)) {
				break;
			}
		}
		if (nodes.empty()) {
			return false;
		}
		idx_t lower = 0;
		idx_t upper = nodes.size() - 1;
		// binary search to find the node
		while (lower <= upper) {
			idx_t index = (lower + upper) / 2;
			D_ASSERT(index < nodes.size());
			auto &entry = nodes[index];
			D_ASSERT(entry.row_start == entry.node->start);
			if (row_number < entry.row_start) {
				upper = index - 1;
			} else if (row_number >= entry.row_start + entry.node->count) {
				lower = index + 1;
			} else {
				result = index;
				return true;
			}
		}
		return false;
	}

	void Verify(SegmentLock &) {
#ifdef DEBUG
		idx_t base_start = nodes.empty() ? 0 : nodes[0].node->start;
		for (idx_t i = 0; i < nodes.size(); i++) {
			D_ASSERT(nodes[i].row_start == nodes[i].node->start);
			D_ASSERT(nodes[i].node->start == base_start);
			base_start += nodes[i].node->count;
		}
#endif
	}
	void Verify() {
#ifdef DEBUG
		auto l = Lock();
		Verify(l);
#endif
	}

	SegmentIterationHelper Segments() {
		return SegmentIterationHelper(*this);
	}

	void Reinitialize() {
		if (nodes.empty()) {
			return;
		}
		idx_t offset = nodes[0].node->start;
		for (auto &entry : nodes) {
			if (entry.node->start != offset) {
				throw InternalException("In SegmentTree::Reinitialize - gap found between nodes!");
			}
			entry.row_start = offset;
			offset += entry.node->count;
		}
	}

protected:
	atomic<bool> finished_loading;

	//! Load the next segment - only used when lazily loading
	virtual unique_ptr<T> LoadSegment() {
		return nullptr;
	}

private:
	//! The nodes in the tree, can be binary searched
	vector<SegmentNode<T>> nodes;
	//! Lock to access or modify the nodes
	mutex node_lock;

private:
	T *GetRootSegmentInternal() {
		return nodes.empty() ? nullptr : nodes[0].node.get();
	}

	class SegmentIterationHelper {
	public:
		explicit SegmentIterationHelper(SegmentTree &tree) : tree(tree) {
		}

	private:
		SegmentTree &tree;

	private:
		class SegmentIterator {
		public:
			SegmentIterator(SegmentTree &tree_p, T *current_p) : tree(tree_p), current(current_p) {
			}

			SegmentTree &tree;
			T *current;

		public:
			void Next() {
				current = tree.GetNextSegment(current);
			}

			SegmentIterator &operator++() {
				Next();
				return *this;
			}
			bool operator!=(const SegmentIterator &other) const {
				return current != other.current;
			}
			T &operator*() const {
				D_ASSERT(current);
				return *current;
			}
		};

	public:
		SegmentIterator begin() { // NOLINT: match stl API
			return SegmentIterator(tree, tree.GetRootSegment());
		}
		SegmentIterator end() { // NOLINT: match stl API
			return SegmentIterator(tree, nullptr);
		}
	};

	//! Load the next segment, if there are any left to load
	bool LoadNextSegment(SegmentLock &l) {
		if (!SUPPORTS_LAZY_LOADING) {
			return false;
		}
		if (finished_loading) {
			return false;
		}
		auto result = LoadSegment();
		if (result) {
			AppendSegmentInternal(l, std::move(result));
			return true;
		}
		return false;
	}

	//! Load all segments, if there are any left to load
	void LoadAllSegments(SegmentLock &l) {
		if (!SUPPORTS_LAZY_LOADING) {
			return;
		}
		while (LoadNextSegment(l)) {
		}
	}
};

} // namespace duckdb
