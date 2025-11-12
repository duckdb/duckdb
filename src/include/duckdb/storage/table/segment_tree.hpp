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
	SegmentNode() : next(nullptr) {
	}

	idx_t row_start;
	unique_ptr<T> node;
	//! The next segment after this one
#ifndef DUCKDB_R_BUILD
	atomic<SegmentNode<T> *> next;
#else
	SegmentNode<T> *next;
#endif
	//! The index within the segment tree
	idx_t index;

public:
	optional_ptr<SegmentNode<T>> Next() {
#ifndef DUCKDB_R_BUILD
		return next.load();
#else
		return next;
#endif
	}
};

//! The SegmentTree maintains a list of all segments of a specific column in a table, and allows searching for a segment
//! by row number
template <class T, bool SUPPORTS_LAZY_LOADING = false>
class SegmentTree {
private:
	class SegmentIterationHelper;
	class SegmentNodeIterationHelper;

public:
	explicit SegmentTree() : finished_loading(true) {
	}
	virtual ~SegmentTree() {
	}

	//! Locks the segment tree. All methods to the segment tree either lock the segment tree, or take an already
	//! obtained lock.
	SegmentLock Lock() const {
		return SegmentLock(node_lock);
	}

	bool IsEmpty(SegmentLock &l) {
		return GetRootSegment(l) == nullptr;
	}

	//! Gets a pointer to the first segment. Useful for scans.
	optional_ptr<SegmentNode<T>> GetRootSegment() {
		auto l = Lock();
		return GetRootSegment(l);
	}

	optional_ptr<SegmentNode<T>> GetRootSegment(SegmentLock &l) {
		if (nodes.empty()) {
			LoadNextSegment(l);
		}
		return GetRootSegmentInternal();
	}
	//! Obtains ownership of the data of the segment tree
	vector<unique_ptr<SegmentNode<T>>> MoveSegments(SegmentLock &l) {
		LoadAllSegments(l);
		return std::move(nodes);
	}
	vector<unique_ptr<SegmentNode<T>>> MoveSegments() {
		auto l = Lock();
		return MoveSegments(l);
	}

	const vector<unique_ptr<SegmentNode<T>>> &ReferenceSegments(SegmentLock &l) {
		LoadAllSegments(l);
		return nodes;
	}
	const vector<unique_ptr<SegmentNode<T>>> &ReferenceSegments() {
		auto l = Lock();
		return ReferenceSegments(l);
	}
	vector<unique_ptr<SegmentNode<T>>> &ReferenceLoadedSegmentsMutable(SegmentLock &l) {
		return nodes;
	}
	const vector<unique_ptr<SegmentNode<T>>> &ReferenceLoadedSegments(SegmentLock &l) const {
		return nodes;
	}

	idx_t GetSegmentCount() {
		auto l = Lock();
		return GetSegmentCount(l);
	}
	idx_t GetSegmentCount(SegmentLock &l) const {
		return nodes.size();
	}
	//! Gets a pointer to the nth segment. Negative numbers start from the back.
	optional_ptr<SegmentNode<T>> GetSegmentByIndex(int64_t index) {
		auto l = Lock();
		return GetSegmentByIndex(l, index);
	}
	optional_ptr<SegmentNode<T>> GetSegmentByIndex(SegmentLock &l, int64_t index) {
		if (index < 0) {
			// load all segments
			LoadAllSegments(l);
			index += nodes.size();
			if (index < 0) {
				return nullptr;
			}
			return nodes[UnsafeNumericCast<idx_t>(index)].get();
		} else {
			// lazily load segments until we reach the specific segment
			while (idx_t(index) >= nodes.size() && LoadNextSegment(l)) {
			}
			if (idx_t(index) >= nodes.size()) {
				return nullptr;
			}
			return nodes[UnsafeNumericCast<idx_t>(index)].get();
		}
	}
	//! Gets the next segment
	optional_ptr<SegmentNode<T>> GetNextSegment(SegmentNode<T> &node) {
		if (!SUPPORTS_LAZY_LOADING) {
			return node.Next();
		}
		if (finished_loading) {
			return node.Next();
		}
		auto l = Lock();
		return GetNextSegment(l, node);
	}
	optional_ptr<SegmentNode<T>> GetNextSegment(SegmentLock &l, SegmentNode<T> &node) {
#ifdef DEBUG
		D_ASSERT(RefersToSameObject(*nodes[node.index], node));
#endif
		return GetSegmentByIndex(l, UnsafeNumericCast<int64_t>(node.index + 1));
	}

	//! Gets a pointer to the last segment. Useful for appends.
	optional_ptr<SegmentNode<T>> GetLastSegment(SegmentLock &l) {
		LoadAllSegments(l);
		if (nodes.empty()) {
			return nullptr;
		}
		return nodes.back().get();
	}
	//! Gets a pointer to a specific column segment for the given row
	optional_ptr<SegmentNode<T>> GetSegment(idx_t row_number) {
		auto l = Lock();
		return GetSegment(l, row_number);
	}
	optional_ptr<SegmentNode<T>> GetSegment(SegmentLock &l, idx_t row_number) {
		return nodes[GetSegmentIndex(l, row_number)].get();
	}

	//! Append a column segment to the tree
	void AppendSegmentInternal(SegmentLock &l, unique_ptr<T> segment) {
		D_ASSERT(segment);
		// add the node to the list of nodes
		auto node = make_uniq<SegmentNode<T>>();
		node->row_start = segment->GetSegmentStart();
		node->node = std::move(segment);
		node->index = nodes.size();
		node->next = nullptr;
		if (!nodes.empty()) {
			nodes.back()->next = node.get();
		}
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
	bool HasSegment(SegmentNode<T> &segment) {
		auto l = Lock();
		return HasSegment(l, segment);
	}
	bool HasSegment(SegmentLock &, SegmentNode<T> &segment) {
		return segment.index < nodes.size() && RefersToSameObject(*nodes[segment.index], segment);
	}

	//! Erase all segments after a specific segment
	void EraseSegments(SegmentLock &l, idx_t segment_start) {
		LoadAllSegments(l);
		if (segment_start >= nodes.size()) {
			return;
		}
		nodes.erase(nodes.begin() + UnsafeNumericCast<int64_t>(segment_start), nodes.end());
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
			error += StringUtil::Format("Node %lld: Start %lld, Count %lld", i, nodes[i]->row_start,
			                            nodes[i]->node->count.load());
		}
		throw InternalException("Could not find node in column segment tree!\n%s", error);
	}

	bool TryGetSegmentIndex(SegmentLock &l, idx_t row_number, idx_t &result) {
		// load segments until the row number is within bounds
		while (nodes.empty() || (row_number >= (nodes.back()->row_start + nodes.back()->node->count))) {
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
			if (index >= nodes.size()) {
				string segments;
				for (auto &entry : nodes) {
					segments += StringUtil::Format("Start %d Count %d", entry->row_start, entry->node->count.load());
				}
				throw InternalException("Segment tree index not found for row number %d\nSegments:%s", row_number,
				                        segments);
			}
			auto &entry = *nodes[index];
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
		idx_t base_start = nodes.empty() ? 0 : nodes[0]->row_start;
		for (idx_t i = 0; i < nodes.size(); i++) {
			D_ASSERT(nodes[i]->row_start == base_start);
			base_start += nodes[i]->node->count;
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

	SegmentIterationHelper Segments(SegmentLock &l) {
		return SegmentIterationHelper(*this, l);
	}

	SegmentNodeIterationHelper SegmentNodes() {
		return SegmentNodeIterationHelper(*this);
	}

	SegmentNodeIterationHelper SegmentNodes(SegmentLock &l) {
		return SegmentNodeIterationHelper(*this, l);
	}

protected:
	atomic<bool> finished_loading;

	//! Load the next segment - only used when lazily loading
	virtual unique_ptr<T> LoadSegment() {
		return nullptr;
	}

	optional_ptr<SegmentNode<T>> GetRootSegmentInternal() const {
		return nodes.empty() ? nullptr : nodes[0].get();
	}

private:
	//! The nodes in the tree, can be binary searched
	vector<unique_ptr<SegmentNode<T>>> nodes;
	//! Lock to access or modify the nodes
	mutable mutex node_lock;

private:
	class BaseSegmentIterator {
	public:
		BaseSegmentIterator(SegmentTree &tree_p, optional_ptr<SegmentNode<T>> current_p, optional_ptr<SegmentLock> lock)
		    : tree(tree_p), current(current_p), lock(lock) {
		}

		SegmentTree &tree;
		optional_ptr<SegmentNode<T>> current;
		optional_ptr<SegmentLock> lock;

	public:
		void Next() {
			current = lock ? tree.GetNextSegment(*lock, *current) : tree.GetNextSegment(*current);
		}

		BaseSegmentIterator &operator++() {
			Next();
			return *this;
		}
		bool operator!=(const BaseSegmentIterator &other) const {
			return current != other.current;
		}
	};
	class SegmentIterationHelper {
	public:
		explicit SegmentIterationHelper(SegmentTree &tree) : tree(tree) {
		}
		SegmentIterationHelper(SegmentTree &tree, SegmentLock &l) : tree(tree), lock(l) {
		}

	private:
		SegmentTree &tree;
		optional_ptr<SegmentLock> lock;

	private:
		class SegmentIterator : public BaseSegmentIterator {
		public:
			SegmentIterator(SegmentTree &tree_p, optional_ptr<SegmentNode<T>> current_p, optional_ptr<SegmentLock> lock)
			    : BaseSegmentIterator(tree_p, current_p, lock) {
			}

			T &operator*() const {
				return *BaseSegmentIterator::current->node;
			}
		};

	public:
		SegmentIterator begin() { // NOLINT: match stl API
			auto root = lock ? tree.GetRootSegment(*lock) : tree.GetRootSegment();
			return SegmentIterator(tree, root, lock);
		}
		SegmentIterator end() { // NOLINT: match stl API
			return SegmentIterator(tree, nullptr, lock);
		}
	};
	class SegmentNodeIterationHelper {
	public:
		explicit SegmentNodeIterationHelper(SegmentTree &tree) : tree(tree) {
		}
		SegmentNodeIterationHelper(SegmentTree &tree, SegmentLock &l) : tree(tree), lock(l) {
		}

	private:
		SegmentTree &tree;
		optional_ptr<SegmentLock> lock;

	private:
		class SegmentIterator : public BaseSegmentIterator {
		public:
			SegmentIterator(SegmentTree &tree_p, optional_ptr<SegmentNode<T>> current_p, optional_ptr<SegmentLock> lock)
			    : BaseSegmentIterator(tree_p, current_p, lock) {
			}

			SegmentNode<T> &operator*() {
				return *BaseSegmentIterator::current;
			}
		};

	public:
		SegmentIterator begin() { // NOLINT: match stl API
			auto root = lock ? tree.GetRootSegment(*lock) : tree.GetRootSegment();
			return SegmentIterator(tree, root, lock);
		}
		SegmentIterator end() { // NOLINT: match stl API
			return SegmentIterator(tree, nullptr, lock);
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
