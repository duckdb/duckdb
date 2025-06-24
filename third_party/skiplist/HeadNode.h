/**
 * @file
 *
 * Project: skiplist
 *
 * Created by Paul Ross on 03/12/2015.
 *
 * Copyright (c) 2015-2023 Paul Ross. All rights reserved.
 *
 * @code
 * MIT License
 *
 * Copyright (c) 2017-2023 Paul Ross
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * @endcode
 */

#ifndef SkipList_HeadNode_h
#define SkipList_HeadNode_h

#include <functional>
//#ifdef SKIPLIST_THREAD_SUPPORT
//    #include <mutex>
//#endif
#include <vector>

#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
#include <sstream>
#endif // INCLUDE_METHODS_THAT_USE_STREAMS

#include "IntegrityEnums.h"

/** HeadNode
 *
 * @brief A HeadNode is a skip list. This is the single node leading to all other content Nodes.
 *
 * Example:
 *
 * @code
 *      OrderedStructs::SkipList::HeadNode<double> sl;
 *      for (int i = 0; i < 100; ++i) {
 *          sl.insert(i * 22.0 / 7.0);
 *      }
 *      sl.size(); // 100
 *      sl.at(50); // Value of 50 pi
 *      sl.remove(sl.at(50)); // Remove 50 pi
 * @endcode
 *
 * Created by Paul Ross on 03/12/2015.
 *
 * Copyright (c) 2015-2023 Paul Ross. All rights reserved.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 */
template <typename T, typename _Compare=std::less<T>>
class HeadNode {
public:
    /**
     * Constructor for and Empty Skip List.
     *
     * @param cmp The comparison function for comparing Node values.
     */
    HeadNode(_Compare cmp=_Compare()) : _count(0), _compare(cmp), _pool(cmp) {
#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
        _dot_file_subgraph = 0;
#endif
    }
    // Const methods
    //
    // Returns true if the value is present in the skip list.
    bool has(const T &value) const;
    // Returns the value at the index in the skip list.
    // Will throw an OrderedStructs::SkipList::IndexError if index out of range.
    const T &at(size_t index) const;
    // Find the value at index and write count values to dest.
    // Will throw a SkipList::IndexError if any index out of range.
    // This is useful for rolling median on even length lists where
    // the caller might want to implement the mean of two values.
    void at(size_t index, size_t count, std::vector<T> &dest) const;
    // Computes index of the first occurrence of a value
    // Will throw a ValueError if the value does not exist in the skip list
    size_t index(const T& value) const;
    // Number of values in the skip list.
    size_t size() const;
    // Non-const methods
    //
    // Insert a value.
    void insert(const T &value);
    // Remove a value and return it.
    // Will throw a ValueError is value not present.
    T remove(const T &value);

    // Const methods that are mostly used for debugging and visualisation.
    //
    // Number of linked lists that are in the skip list.
    size_t height() const;
    // Number of linked lists that the node at index has.
    // Will throw a SkipList::IndexError if idx out of range.
    size_t height(size_t idx) const;
    // The skip width of the node at index has.
    // May throw a SkipList::IndexError
    size_t width(size_t idx, size_t level) const;

#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
    void dotFile(std::ostream &os) const;
    void dotFileFinalise(std::ostream &os) const;
#endif // INCLUDE_METHODS_THAT_USE_STREAMS

    // Returns non-zero if the integrity of this data structure is compromised
    // This is a thorough but expensive check!
    IntegrityCheck lacksIntegrity() const;
    // Estimate of the number of bytes used by the skip list
    size_t size_of() const;
    virtual ~HeadNode();

protected:
    void _adjRemoveRefs(size_t level, Node<T, _Compare> *pNode);
    const Node<T, _Compare> *_nodeAt(size_t idx) const;

protected:
    // Standardised way of throwing a ValueError
    void _throwValueErrorNotFound(const T &value) const;
    void _throwIfValueDoesNotCompare(const T &value) const;
    // Internal integrity checks
    IntegrityCheck _lacksIntegrityCyclicReferences() const;
    IntegrityCheck _lacksIntegrityWidthAccumulation() const;
    IntegrityCheck _lacksIntegrityNodeReferencesNotInList() const;
    IntegrityCheck _lacksIntegrityOrder() const;
protected:
    /// Number of nodes in the list.
    size_t _count;
    /// My node references, the size of this is the largest height in the list
    SwappableNodeRefStack<T, _Compare> _nodeRefs;
    /// Comparison function.
    _Compare _compare;
    typename Node<T, _Compare>::_Pool _pool;
#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
    /// Used to count how many sub-graphs have been plotted
    mutable size_t _dot_file_subgraph;
#endif

private:
    /// Prevent cctor and operator=
    HeadNode(const HeadNode &that);
    HeadNode &operator=(const HeadNode &that) const;
};

/**
 * Returns true if the value is present in the skip list.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value Value to check if it is in the Skip List.
 * @return true if in the Skip List.
 */
template <typename T, typename _Compare>
bool HeadNode<T, _Compare>::has(const T &value) const {
    _throwIfValueDoesNotCompare(value);
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    for (size_t l = _nodeRefs.height(); l-- > 0;) {
        assert(_nodeRefs[l].pNode);
        if (_nodeRefs[l].pNode->has(value)) {
            return true;
        }
    }
    return false;
}

/**
 * Returns the value at a particular index.
 * Will throw an OrderedStructs::SkipList::IndexError if index out of range.
 *
 * If @ref SKIPLIST_THREAD_SUPPORT is defined this will block.
 *
 * See _throw_exceeds_size() that does the throw.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param index The index.
 * @return The value at that index.
 */
template <typename T, typename _Compare>
const T &HeadNode<T, _Compare>::at(size_t index) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    const Node<T, _Compare> *pNode = _nodeAt(index);
    assert(pNode);
    return pNode->value();
}

/**
 * Find the count number of value starting at index and write them to dest.
 *
 * Will throw a OrderedStructs::SkipList::IndexError if any index out of range.
 *
 * This is useful for rolling median on even length lists where the caller might want to implement the mean of two
 * values.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param index The index.
 * @param count The number of values to retrieve.
 * @param dest The vector of values
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::at(size_t index, size_t count,
                               std::vector<T> &dest) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    dest.clear();
    const Node<T, _Compare> *pNode = _nodeAt(index);
    // _nodeAt will (should) throw an IndexError so this
    // assert should always be true
    assert(pNode);
    while (count) {
        if (! pNode) {
            _throw_exceeds_size(_count);
        }
        dest.push_back(pNode->value());
        pNode = pNode->next();
        --count;
    }
}

/**
 * Computes index of the first occurrence of a value
 * Will throw a OrderedStructs::SkipList::ValueError if the value does not exist in the skip list
 * Will throw a OrderedStructs::SkipList::FailedComparison if the value is not comparable.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value The value to search for.
 * @return
 */
template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::index(const T& value) const {
    _throwIfValueDoesNotCompare(value);
    size_t idx;

#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    for (size_t l = _nodeRefs.height(); l-- > 0;) {
        assert(_nodeRefs[l].pNode);
        if (_nodeRefs[l].pNode->index(value, idx, l)) {
            idx += _nodeRefs[l].width;
            assert(idx > 0);
            return idx - 1;
        }
    }
    _throwValueErrorNotFound(value);
    return 0;
}

/**
 * Return the number of values in the Skip List.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return The number of values in the Skip List.
 */
template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::size() const {
    return _count;
}

template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::height() const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    size_t val = _nodeRefs.height();
    return val;
}

/**
 * Return the number of linked lists that the node at index has.
 *
 * Will throw a OrderedStructs::SkipList::IndexError if the index out of range.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param idx The index of the Skip List node.
 * @return The number of linked lists that the node at the index has.
 */
template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::height(size_t idx) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    const Node<T, _Compare> *pNode = _nodeAt(idx);
    assert(pNode);
    return pNode->height();
}

/**
 * The skip width of the Node at index has at the given level.
 * Will throw an IndexError if the index is out of range.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param idx The index.
 * @param level The level.
 * @return Width of Node.
 */
template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::width(size_t idx, size_t level) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    // Will throw if out of range.
    const Node<T, _Compare> *pNode = _nodeAt(idx);
    assert(pNode);
    if (level >= pNode->height()) {
        _throw_exceeds_size(pNode->height());
    }
    return pNode->nodeRefs()[level].width;
}

/**
 * Find the Node at the given index.
 * Will throw an IndexError if the index is out of range.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param idx The index.
 * @return The Node.
 */
template <typename T, typename _Compare>
const Node<T, _Compare> *HeadNode<T, _Compare>::_nodeAt(size_t idx) const {
    if (idx < _count) {
        for (size_t l = _nodeRefs.height(); l-- > 0;) {
            if (_nodeRefs[l].pNode && _nodeRefs[l].width <= idx + 1) {
                size_t new_index = idx + 1 - _nodeRefs[l].width;
                const Node<T, _Compare> *pNode = _nodeRefs[l].pNode->at(new_index);
                if (pNode) {
                    return pNode;
                }
            }
        }
    }
    assert(idx >= _count);
    _throw_exceeds_size(_count);
    // Should not get here as _throw_exceeds_size() will always throw.
    return NULL;
}

/**
 * Insert a value.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::insert(const T &value) {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#ifdef SKIPLIST_THREAD_SUPPORT_TRACE
    std::cout << "HeadNode insert() thread: " << std::this_thread::get_id() << std::endl;
#endif
#endif
    Node<T, _Compare> *pNode = nullptr;
    size_t level = _nodeRefs.height();

    _throwIfValueDoesNotCompare(value);
    while (level-- > 0) {
        assert(_nodeRefs[level].pNode);
        pNode = _nodeRefs[level].pNode->insert(value);
        if (pNode) {
            break;
        }
    }
    if (! pNode) {
        pNode = _pool.Allocate(value);
        level = 0;
    }
    assert(pNode);
    SwappableNodeRefStack<T, _Compare> &thatRefs = pNode->nodeRefs();
    if (thatRefs.canSwap()) {
        // Expand this to that
        while (_nodeRefs.height() < thatRefs.height()) {
            _nodeRefs.push_back(nullptr, _count + 1);
        }
        if (level < thatRefs.swapLevel()) {
            // Happens when we were originally, say 3 high (max height of any
            // previously seen node). Then a node is created
            // say 5 high. In that case this will be at level 2 and
            // thatRefs.swapLevel() will be 3
            assert(level + 1 == thatRefs.swapLevel());
            thatRefs[thatRefs.swapLevel()].width += _nodeRefs[level].width;
            ++level;
        }
        // Now swap
        while (level < _nodeRefs.height() && thatRefs.canSwap()) {
            assert(thatRefs.canSwap());
            assert(level == thatRefs.swapLevel());
            _nodeRefs[level].width -= thatRefs[level].width - 1;
            thatRefs.swap(_nodeRefs);
            if (thatRefs.canSwap()) {
                assert(thatRefs[thatRefs.swapLevel()].width == 0);
                thatRefs[thatRefs.swapLevel()].width = _nodeRefs[level].width;
            }
            ++level;
        }
        // Check all references swapped
        assert(! thatRefs.canSwap());
        // Check that all 'this' pointers created on construction have been moved
        assert(thatRefs.noNodePointerMatches(pNode));
    }
    if (level < thatRefs.swapLevel()) {
        // Happens when we are, say 5 high then a node is created
        // and consumed by the next node say 3 high. In that case this will be
        // at level 2 and thatRefs.swapLevel() will be 3
        assert(level + 1 == thatRefs.swapLevel());
        ++level;
    }
    // Increment my widths as my references are now going over the top of
    // pNode.
    while (level < _nodeRefs.height() && level >= thatRefs.height()) {
        _nodeRefs[level++].width += 1;
    }
    ++_count;
#ifdef SKIPLIST_THREAD_SUPPORT
#ifdef SKIPLIST_THREAD_SUPPORT_TRACE
    std::cout << "HeadNode insert() thread: " << std::this_thread::get_id() << " DONE" << std::endl;
#endif
#endif
}

/**
 * Adjust references >= level for removal of the node pNode.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param level Current level.
 * @param pNode Node to swap references with.
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::_adjRemoveRefs(size_t level,
                                           Node<T, _Compare> *pNode) {
    assert(pNode);
    SwappableNodeRefStack<T, _Compare> &thatRefs = pNode->nodeRefs();

    // Swap all remaining levels
    // This assertion checks that if swapping can take place we must be at the
    // same level.
    assert(! thatRefs.canSwap() || level == thatRefs.swapLevel());
    while (level < _nodeRefs.height() && thatRefs.canSwap()) {
        assert(level == thatRefs.swapLevel());
        // Compute the new width for the new node
        thatRefs[level].width += _nodeRefs[level].width - 1;
        thatRefs.swap(_nodeRefs);
        ++level;
        if (! thatRefs.canSwap()) {
            break;
        }
    }
    assert(! thatRefs.canSwap());
    // Decrement my widths as my references are now going over the top of
    // pNode.
    while (level < _nodeRefs.height()) {
        _nodeRefs[level++].width -= 1;
    }
    // Decrement my stack while top has a NULL pointer.
    while (_nodeRefs.height() && ! _nodeRefs[_nodeRefs.height() - 1].pNode) {
        _nodeRefs.pop_back();
    }
}

/**
 * Remove a Node with a value.
 * May throw a ValueError if the value is not found.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value The value in the Node to remove.
 * @return The value removed.
 */
template <typename T, typename _Compare>
T HeadNode<T, _Compare>::remove(const T &value) {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#ifdef SKIPLIST_THREAD_SUPPORT_TRACE
    std::cout << "HeadNode remove() thread: " << std::this_thread::get_id() << std::endl;
#endif
#endif
    Node<T, _Compare> *pNode = nullptr;
    size_t level;

    _throwIfValueDoesNotCompare(value);
    for (level = _nodeRefs.height(); level-- > 0;) {
        assert(_nodeRefs[level].pNode);
        pNode = _nodeRefs[level].pNode->remove(level, value);
        if (pNode) {
            break;
        }
    }
    if (! pNode) {
        _throwValueErrorNotFound(value);
    }
    // Take swap level as some swaps will have been dealt with by the remove() above.
    _adjRemoveRefs(pNode->nodeRefs().swapLevel(), pNode);
    --_count;
    T ret_val = _pool.Release(pNode);
#ifdef SKIPLIST_THREAD_SUPPORT_TRACE
    std::cout << "HeadNode remove() thread: " << std::this_thread::get_id() << " DONE" << std::endl;
#endif
    return ret_val;
}

/**
 * Throw a ValueError in a consistent fashion.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value The value to put into the ValueError.
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::_throwValueErrorNotFound(const T &value) const {
#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
    std::ostringstream oss;
    oss << "Value " << value << " not found.";
    std::string err_msg = oss.str();
#else
    std::string err_msg = "Value not found.";
#endif
    throw ValueError(err_msg);
}

/**
 * Checks that the value == value.
 * This will throw a FailedComparison if that is not the case, for example NaN.
 *
 * @note
 * The Node class is (should be) not directly accessible by the user so we can just assert(value == value) in Node.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param value
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::_throwIfValueDoesNotCompare(const T &value) const {
    if (value != value) {
        throw FailedComparison(
            "Can not work with something that does not compare equal to itself.");
    }
}

/**
 * This tests that at every level >= 0 the sequence of node pointers
 * at that level does not contain a cyclic reference.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck HeadNode<T, _Compare>::_lacksIntegrityCyclicReferences() const {
    assert(_nodeRefs.height());
    // Check for cyclic references at each level
    for (size_t level = 0; level < _nodeRefs.height(); ++level) {
        Node<T, _Compare> *p1 = _nodeRefs[level].pNode;
        Node<T, _Compare> *p2 = _nodeRefs[level].pNode;
        while (p1 && p2) {
            p1 = p1->nodeRefs()[level].pNode;
            if (p2->nodeRefs()[level].pNode) {
                p2 = p2->nodeRefs()[level].pNode->nodeRefs()[level].pNode;
            } else {
                p2 = nullptr;
            }
            if (p1 && p2 && p1 == p2) {
                return HEADNODE_DETECTS_CYCLIC_REFERENCE;
            }
        }
    }
    return INTEGRITY_SUCCESS;
}

/**
 * This tests that at every level > 0 the node to node width is the same
 * as the accumulated node to node widths at level - 1.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck HeadNode<T, _Compare>::_lacksIntegrityWidthAccumulation() const {
    assert(_nodeRefs.height());
    for (size_t level = 1; level < _nodeRefs.height(); ++level) {
        const Node<T, _Compare> *pl = _nodeRefs[level].pNode;
        const Node<T, _Compare> *pl_1 = _nodeRefs[level - 1].pNode;
        assert(pl && pl_1); // No nulls allowed in HeadNode
        size_t wl = _nodeRefs[level].width;
        size_t wl_1 = _nodeRefs[level - 1].width;
        while (true) {
            while (pl != pl_1) {
                assert(pl_1); // Could only happen if a lower reference was NULL and the higher non-NULL.
                wl_1 += pl_1->width(level - 1);
                pl_1 = pl_1->pNode(level - 1);
            }
            if (wl != wl_1) {
                return HEADNODE_LEVEL_WIDTHS_MISMATCH;
            }
            if (pl == nullptr && pl_1 == nullptr) {
                break;
            }
            wl = pl->width(level);
            wl_1 = pl_1->width(level - 1);
            pl = pl->pNode(level);
            pl_1 = pl_1->pNode(level - 1);
        }
    }
    return INTEGRITY_SUCCESS;
}

/**
 * This tests the integrity of each Node.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck HeadNode<T, _Compare>::_lacksIntegrityNodeReferencesNotInList() const {
    assert(_nodeRefs.height());

    IntegrityCheck result;
    std::set<const Node<T, _Compare>*> nodeSet;
    const Node<T, _Compare> *pNode = _nodeRefs[0].pNode;
    assert(pNode);

    // First gather all nodes, slightly awkward code here is so that
    // NULL is always included.
    nodeSet.insert(pNode);
    do {
        pNode = pNode->next();
        nodeSet.insert(pNode);
    } while (pNode);
    assert(nodeSet.size() == _count + 1); // All nodes plus NULL
    // Then test each node does not have pointers that are not in nodeSet
    pNode = _nodeRefs[0].pNode;
    while (pNode) {
        result = pNode->lacksIntegrityRefsInSet(nodeSet);
        if (result) {
            return result;
        }
        pNode = pNode->next();
    }
    return INTEGRITY_SUCCESS;
}

/**
 * Integrity check. Traverse the lowest level and check that the ordering
 * is correct according to the compare function. The HeadNode checks that the
 * Node(s) have correctly applied the compare function.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck HeadNode<T, _Compare>::_lacksIntegrityOrder() const {
    if (_nodeRefs.height()) {
        // Traverse the lowest level list iteratively deleting as we go
        // Doing this recursivley could be expensive as we are at level 0.
        const Node<T, _Compare> *node = _nodeRefs[0].pNode;
        const Node<T, _Compare> *next;
        while (node) {
            next = node->next();
            if (next && _compare(next->value(), node->value())) {
                return HEADNODE_DETECTS_OUT_OF_ORDER;
            }
            node = next;
        }
    }
    return INTEGRITY_SUCCESS;
}

/**
 * Full integrity check.
 * This calls the other integrity check functions.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck HeadNode<T, _Compare>::lacksIntegrity() const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    if (_nodeRefs.height()) {
        IntegrityCheck result = _nodeRefs.lacksIntegrity();
        if (result) {
            return result;
        }
        if (! _nodeRefs.noNodePointerMatches(nullptr)) {
            return HEADNODE_CONTAINS_NULL;
        }
        // Check all nodes for integrity
        const Node<T, _Compare> *pNode = _nodeRefs[0].pNode;
        while (pNode) {
            result = pNode->lacksIntegrity(_nodeRefs.height());
            if (result) {
                return result;
            }
            pNode = pNode->next();
        }
        // Check count against total number of nodes
        pNode = _nodeRefs[0].pNode;
        size_t total = 0;
        while (pNode) {
            total += pNode->nodeRefs()[0].width;
            pNode = pNode->next();
        }
        if (total != _count) {
            return HEADNODE_COUNT_MISMATCH;
        }
        result = _lacksIntegrityWidthAccumulation();
        if (result) {
            return result;
        }
        result = _lacksIntegrityCyclicReferences();
        if (result) {
            return result;
        }
        result = _lacksIntegrityNodeReferencesNotInList();
        if (result) {
            return result;
        }
        result = _lacksIntegrityOrder();
        if (result) {
            return result;
        }
    }
    return INTEGRITY_SUCCESS;
}

/**
 * Returns an estimate of the memory usage of an instance.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @return The size of the memory estimate.
 */
template <typename T, typename _Compare>
size_t HeadNode<T, _Compare>::size_of() const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    // sizeof(*this) includes the size of _nodeRefs but _nodeRefs.size_of()
    // includes sizeof(_nodeRefs) so we need to subtract to avoid double counting
    size_t ret_val = sizeof(*this) + _nodeRefs.size_of() - sizeof(_nodeRefs);
    if (_nodeRefs.height()) {
        const Node<T, _Compare> *node = _nodeRefs[0].pNode;
        while (node) {
            ret_val += node->size_of();
            node = node->next();
        }
    }
    return ret_val;
}

/**
 * Destructor.
 * This deletes all Nodes.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 */
template <typename T, typename _Compare>
HeadNode<T, _Compare>::~HeadNode() {
    // Hmm could this deadlock?
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    if (_nodeRefs.height()) {
        // Traverse the lowest level list iteratively deleting as we go
        // Doing this recursivley could be expensive as we are at level 0.
        const Node<T, _Compare> *node = _nodeRefs[0].pNode;
        const Node<T, _Compare> *next;
        while (node) {
            next = node->next();
            delete node;
            --_count;
            node = next;
        }
    }
    assert(_count == 0);
}

#ifdef INCLUDE_METHODS_THAT_USE_STREAMS

/**
 * Create a DOT file of the internal representation.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param os Where to write the DOT file.
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::dotFile(std::ostream &os) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    if (_dot_file_subgraph == 0) {
        os << "digraph SkipList {" << std::endl;
        os << "label = \"SkipList.\"" << std::endl;
        os << "graph [rankdir = \"LR\"];" << std::endl;
        os << "node [fontsize = \"12\" shape = \"ellipse\"];" << std::endl;
        os << "edge [];" << std::endl;
        os << std::endl;
    }
    os << "subgraph cluster" << _dot_file_subgraph << " {" << std::endl;
    os << "style=dashed" << std::endl;
    os << "label=\"Skip list iteration " << _dot_file_subgraph << "\"" << std::endl;
    os << std::endl;
    os << "\"HeadNode" << _dot_file_subgraph;
    os << "\" [" << std::endl;
    os << "label = \"";
    // Write out the fields
    if (_nodeRefs.height()) {
        for (size_t level = _nodeRefs.height(); level-- > 0;) {
            os << "{ " << _nodeRefs[level].width << " | ";
            os << "<f" << level + 1 << "> ";
            os << std::hex << _nodeRefs[level].pNode << std::dec;
            os << "}";
            if (level > 0) {
                os << " | ";
            }
        }
    } else {
        os << "Empty HeadNode";
    }
    os << "\"" << std::endl;
    os << "shape = \"record\"" << std::endl;
    os << "];" << std::endl;
    // Edges for head node
    for (size_t level = 0; level < _nodeRefs.height(); ++level) {
        os << "\"HeadNode";
        os << _dot_file_subgraph;
        os << "\":f" << level + 1 << " -> ";
        _nodeRefs[level].pNode->writeNode(os, _dot_file_subgraph);
        os << ":w" << level + 1 << " [];" << std::endl;
    }
    os << std::endl;
    // Now all nodes via level 0, if non-empty
    if (_nodeRefs.height()) {
        Node<T, _Compare> *pNode = this->_nodeRefs[0].pNode;
        pNode->dotFile(os, _dot_file_subgraph);
    }
    os << std::endl;
    // NULL, the sentinal node
    if (_nodeRefs.height()) {
        os << "\"node";
        os << _dot_file_subgraph;
        os << "0x0\" [label = \"";
        for (size_t level = _nodeRefs.height(); level-- > 0;) {
            os << "<w" << level + 1 << "> NULL";
            if (level) {
                os << " | ";
            }
        }
        os << "\" shape = \"record\"];" << std::endl;
    }
    // End: "subgraph cluster1 {"
    os << "}" << std::endl;
    os << std::endl;
    _dot_file_subgraph += 1;
}

/**
 * Finalise the DOT file of the internal representation.
 *
 * @tparam T Type of the values in the Skip List.
 * @tparam _Compare Compare function.
 * @param os Where to write the DOT file.
 */
template <typename T, typename _Compare>
void HeadNode<T, _Compare>::dotFileFinalise(std::ostream &os) const {
#ifdef SKIPLIST_THREAD_SUPPORT
    std::lock_guard<std::mutex> lock(gSkipListMutex);
#endif
    if (_dot_file_subgraph > 0) {
        // Link the nodes together with an invisible node.
        //    node0 [shape=record, label = "<f0> | <f1> | <f2> | <f3> | <f4> | <f5> | <f6> | <f7> | <f8> | ",
        //           style=invis,
        //           width=0.01];
        os << "node0 [shape=record, label = \"";
        for (size_t i = 0; i < _dot_file_subgraph; ++i) {
            os << "<f" << i << "> | ";
        }
        os << "\", style=invis, width=0.01];" << std::endl;
        // Now:
        //    node0:f0 -> HeadNode [style=invis];
        //    node0:f1 -> HeadNode1 [style=invis];
        for (size_t i = 0; i < _dot_file_subgraph; ++i) {
            os << "node0:f" << i << " -> HeadNode" << i;
            os << " [style=invis];" << std::endl;
        }
        _dot_file_subgraph = 0;
    }
    os << "}" << std::endl;
}

#endif // INCLUDE_METHODS_THAT_USE_STREAMS

/************************** END: HeadNode *******************************/

#endif // SkipList_HeadNode_h
