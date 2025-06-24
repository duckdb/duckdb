/**
 * @file
 *
 * Project: skiplist
 *
 * Concurrency Tests.
 *
 * Created by Paul Ross on 03/12/2015.
 *
 * Copyright (c) 2015-2023 Paul Ross. All rights reserved.
 *
 * @code
 * MIT License
 *
 * Copyright (c) 2015-2023 Paul Ross
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

#ifndef SkipList_Node_h
#define SkipList_Node_h

#include "IntegrityEnums.h"

#if __cplusplus < 201103L
#define nullptr NULL
#endif

/**************************** Node *********************************/

/**
 * @brief A single node in a Skip List containing a value and references to other downstream Node objects.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 */
template <typename T, typename _Compare>
class Node {
public:
	struct _Pool {
		explicit _Pool(_Compare _cmp) : _compare(_cmp), cache(nullptr) {
		}
		~_Pool() {
			delete cache;
		}
		Node *Allocate(const T &value) {
			if (cache) {
				Node *result = cache;
				cache = nullptr;
				result->Initialize(value);
				return result;
			}

			return new Node(value, _compare, *this);
		}

		T Release(Node *pNode) {
			T result = pNode->value();
			std::swap(pNode, cache);
			delete pNode;
			return result;
		}

		_Compare _compare;
		Node* cache;
		pcg32_fast prng;
	};

    Node(const T &value, _Compare _cmp, _Pool &pool);
    // Const methods
    //
    /// Returns the node value
    const T &value() const { return _value; }
    // Returns true if the value is present in the skip list from this node onwards.
    bool has(const T &value) const;
    // Returns the value at the index in the skip list from this node onwards.
    // Will return nullptr is not found.
    const Node<T, _Compare> *at(size_t idx) const;
    // Computes index of the first occurrence of a value
    bool index(const T& value, size_t &idx, size_t level) const;
    /// Number of linked lists that this node engages in, minimum 1.
    size_t height() const { return _nodeRefs.height(); }
    // Return the pointer to the next node at level 0
    const Node<T, _Compare> *next() const;
    // Return the width at given level.
    size_t width(size_t level) const;
    // Return the node pointer at given level, only used for HeadNode
    // integrity checks.
    const Node<T, _Compare> *pNode(size_t level) const;

    // Non-const methods
    /// Get a reference to the node references
    SwappableNodeRefStack<T, _Compare> &nodeRefs() { return _nodeRefs; }
    /// Get a reference to the node references
    const SwappableNodeRefStack<T, _Compare> &nodeRefs() const { return _nodeRefs; }
    // Insert a node
    Node<T, _Compare> *insert(const T &value);
    // Remove a node
    Node<T, _Compare> *remove(size_t call_level, const T &value);
    // An estimate of the number of bytes used by this node
    size_t size_of() const;

#ifdef INCLUDE_METHODS_THAT_USE_STREAMS
    void dotFile(std::ostream &os, size_t suffix = 0) const;
    void writeNode(std::ostream &os, size_t suffix = 0) const;
#endif // INCLUDE_METHODS_THAT_USE_STREAMS

    // Integrity checks, returns non-zero on failure
    IntegrityCheck lacksIntegrity(size_t headnode_height) const;
    IntegrityCheck lacksIntegrityRefsInSet(const std::set<const Node<T, _Compare>*> &nodeSet) const;

protected:
    Node<T, _Compare> *_adjRemoveRefs(size_t level, Node<T, _Compare> *pNode);

	void Initialize(const T &value) {
		_value = value;
		_nodeRefs.clear();
		do {
			_nodeRefs.push_back(this, _nodeRefs.height() ? 0 : 1);
		} while (_pool.prng() < _pool.prng.max() / 2);
	}

protected:
    T _value;
    SwappableNodeRefStack<T, _Compare> _nodeRefs;
    // Comparison function
    _Compare _compare;
    _Pool &_pool;
private:
    // Prevent cctor and operator=
    Node(const Node &that);
    Node &operator=(const Node &that) const;
};

/**
 * Constructor.
 * This also creates a SwappableNodeRefStack of random height by tossing a virtual coin.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param value The value of the Node.
 * @param _cmp The comparison function.
 */
template <typename T, typename _Compare>
Node<T, _Compare>::Node(const T &value, _Compare _cmp, _Pool &pool) : \
    _value(value), _compare(_cmp), _pool(pool) {
    Initialize(value);
}

/**
 * Returns true if the value is present in the skip list from this node onwards.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param value The value to look for.
 * @return true if the value is present in the skip list from this node onwards.
 */
template <typename T, typename _Compare>
bool Node<T, _Compare>::has(const T &value) const {
    assert(_nodeRefs.height());
    assert(value == value); // value can not be NaN for example
    // Effectively: if (value > _value) {
    if (_compare(_value, value)) {
        for (size_t l = _nodeRefs.height(); l-- > 0;) {
            if (_nodeRefs[l].pNode && _nodeRefs[l].pNode->has(value)) {
                return true;
            }
        }
        return false;
    }
    // Effectively: return value == _value; // false if value smaller
    return !_compare(value, _value) && !_compare(_value, value);
}

/**
 * Return a pointer to the n'th node.
 * Start (or continue) from the highest level, drop down a level if not found.
 * Return nullptr if not found at level 0.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param idx The index from hereon. If zero return this.
 * @return Pointer to the Node or nullptr.
 */
template <typename T, typename _Compare>
const Node<T, _Compare> *Node<T, _Compare>::at(size_t idx) const {
    assert(_nodeRefs.height());
    if (idx == 0) {
        return this;
    }
    for (size_t l = _nodeRefs.height(); l-- > 0;) {
        if (_nodeRefs[l].pNode && _nodeRefs[l].width <= idx) {
            return _nodeRefs[l].pNode->at(idx - _nodeRefs[l].width);
        }
    }
    return nullptr;
}

/**
 * Computes index of the first occurrence of a value.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param value The value to find.
 * @param idx The current index, this will be updated.
 * @param level The current level to search from.
 * @return true if found, false otherwise.
 */
template <typename T, typename _Compare>
bool Node<T, _Compare>::index(const T& value, size_t &idx, size_t level) const {
    assert(_nodeRefs.height());
    assert(value == value); // value can not be NaN for example
    assert(level < _nodeRefs.height());
    // Search has overshot, try again at a lower level.
    //if (_value > value) {
    if (_compare(value, _value)) {
        return false;
    }
    // First check if we match but we have been approached at a high level
    // as there may be an earlier node of the same value but with fewer
    // node references. In that case this search has to fail and try at a
    // lower level.
    // If however the level is 0 and we match then set the idx to 0 to mark us.
    // Effectively: if (_value == value) {
    if (!_compare(value, _value) && !_compare(_value, value)) {
        if (level > 0) {
            return false;
        }
        idx = 0;
        return true;
    }
    // Now work our way down
    // NOTE: We initialise l as level + 1 because l-- > 0 will decrement it to
    // the correct initial value
    for (size_t l = level + 1; l-- > 0;) {
        assert(l < _nodeRefs.height());
        if (_nodeRefs[l].pNode && _nodeRefs[l].pNode->index(value, idx, l)) {
            idx += _nodeRefs[l].width;
            return true;
        }
    }
    return false;
}

/**
 * Return the pointer to the next node at level 0.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @return The next node at level 0.
 */
template <typename T, typename _Compare>
const Node<T, _Compare> *Node<T, _Compare>::next() const {
    assert(_nodeRefs.height());
    return _nodeRefs[0].pNode;
}

/**
 * Return the width at given level.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param level The requested level.
 * @return The width.
 */
template <typename T, typename _Compare>
size_t Node<T, _Compare>::width(size_t level) const {
    assert(level < _nodeRefs.height());
    return _nodeRefs[level].width;
}

/**
 * Return the node pointer at given level, only used for HeadNode integrity checks.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param level The requested level.
 * @return The Node.
 */
template <typename T, typename _Compare>
const Node<T, _Compare> *Node<T, _Compare>::pNode(size_t level) const {
    assert(level < _nodeRefs.height());
    return _nodeRefs[level].pNode;
}

/**
 * Insert a new node with a value.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param value The value of the Node to insert.
 * @return Pointer to the new Node or nullptr on failure.
 */
template <typename T, typename _Compare>
Node<T, _Compare> *Node<T, _Compare>::insert(const T &value) {
    assert(_nodeRefs.height());
    assert(_nodeRefs.noNodePointerMatches(this));
    assert(! _nodeRefs.canSwap());
    assert(value == value); // NaN check for double

    // Effectively: if (value < _value) {
    if (_compare(value, _value)) {
        return nullptr;
    }
    // Recursive search for where to put the node
    Node<T, _Compare> *pNode = nullptr;
    size_t level = _nodeRefs.height();
    // Effectively: if (value >= _value) {
    if (! _compare(value, _value)) {
        for (level = _nodeRefs.height(); level-- > 0;) {
            if (_nodeRefs[level].pNode) {
                pNode = _nodeRefs[level].pNode->insert(value);
                if (pNode) {
                    break;
                }
            }
        }
    }
    // Effectively: if (! pNode && value >= _value) {
    if (! pNode && !_compare(value, _value)) {
        // Insert new node here
        pNode = _pool.Allocate(value);
        level = 0;
    }
    assert(pNode); // Should never get here unless a NaN has slipped through
    // Adjust references by marching up and recursing back.
    SwappableNodeRefStack<T, _Compare> &thatRefs = pNode->_nodeRefs;
    if (! thatRefs.canSwap()) {
        // Have an existing node or new node that is all swapped.
        // All I need to do is adjust my overshooting nodes and return
        // this for the caller to do the same.
        level = thatRefs.height();
        while (level < _nodeRefs.height()) {
            _nodeRefs[level].width += 1;
            ++level;
        }
        // The caller just has to increment its references that overshoot this
        assert(! _nodeRefs.canSwap());
        return this;
    }
    // March upwards
    if (level < thatRefs.swapLevel()) {
        assert(level == thatRefs.swapLevel() - 1);
        // This will happen when say a 3 high node, A, finds a 2 high
        // node, B, that creates a new 2+ high node. A will be at
        // level 1 and the new node will have swapLevel == 2 after
        // B has swapped.
        // Add the level to the accumulator at the next level
        thatRefs[thatRefs.swapLevel()].width += _nodeRefs[level].width;
        ++level;
    }
    size_t min_height = std::min(_nodeRefs.height(), thatRefs.height());
    while (level < min_height) {
        assert(thatRefs.canSwap());
        assert(level == thatRefs.swapLevel());
        assert(level < thatRefs.height());
        assert(_nodeRefs[level].width > 0);
        assert(thatRefs[level].width > 0);
        _nodeRefs[level].width -= thatRefs[level].width - 1;
        assert(_nodeRefs[level].width > 0);
        thatRefs.swap(_nodeRefs);
        if (thatRefs.canSwap()) {
            assert(thatRefs[thatRefs.swapLevel()].width == 0);
            thatRefs[thatRefs.swapLevel()].width = _nodeRefs[level].width;
        }
        ++level;
    }
    // Upwards march complete, now recurse back ('left').
    if (! thatRefs.canSwap()) {
        // All done with pNode locally.
        assert(level == thatRefs.height());
        assert(thatRefs.height() <= _nodeRefs.height());
        assert(level == thatRefs.swapLevel());
        // Adjust my overshooting nodes
        while (level < _nodeRefs.height()) {
            _nodeRefs[level].width += 1;
            ++level;
        }
        // The caller just has to increment its references that overshoot this
        assert(! _nodeRefs.canSwap());
        pNode = this;
    }
    return pNode;
}

/**
 * Adjust the Node references.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param level The level of the caller's node.
 * @param pNode The Node to swap references with.
 * @return The Node with swapped references.
 */
template <typename T, typename _Compare>
Node<T, _Compare> *Node<T, _Compare>::_adjRemoveRefs(size_t level, Node<T, _Compare> *pNode) {
    assert(pNode);
    SwappableNodeRefStack<T, _Compare> &thatRefs = pNode->_nodeRefs;

    assert(pNode != this);
    if (level < thatRefs.swapLevel()) {
        assert(level == thatRefs.swapLevel() - 1);
        ++level;
    }
    if (thatRefs.canSwap()) {
        assert(level == thatRefs.swapLevel());
        while (level < _nodeRefs.height() && thatRefs.canSwap()) {
            assert(level == thatRefs.swapLevel());
            // Compute the new width for the new node
            thatRefs[level].width += _nodeRefs[level].width - 1;
            thatRefs.swap(_nodeRefs);
            ++level;
        }
        assert(thatRefs.canSwap() || thatRefs.allNodePointerMatch(pNode));
    }
    // Decrement my widths as my refs are over the top of the missing pNode.
    while (level < _nodeRefs.height()) {
        _nodeRefs[level].width -= 1;
        ++level;
        thatRefs.incSwapLevel();
    }
    assert(! _nodeRefs.canSwap());
    return pNode;
}

/**
 * Remove a Node with the given value to be removed.
 * The return value must be deleted, the other Nodes have been adjusted as required.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param call_level Level the caller Node is at.
 * @param value Value of the detached Node to remove.
 * @return A pointer to the Node to be free'd or nullptr on failure.
 */
template <typename T, typename _Compare>
Node<T, _Compare> *Node<T, _Compare>::remove(size_t call_level,
                         const T &value) {
    assert(_nodeRefs.height());
    assert(_nodeRefs.noNodePointerMatches(this));

    Node<T, _Compare> *pNode = nullptr;
    // Effectively: if (value >= _value) {
    if (!_compare(value, _value)) {
        for (size_t level = call_level + 1; level-- > 0;) {
            if (_nodeRefs[level].pNode) {
                // Make progress to the right
                pNode = _nodeRefs[level].pNode->remove(level, value);
                if (pNode) {
                    return _adjRemoveRefs(level, pNode);
                }
            }
            // Make progress down
        }
    }
    if (! pNode) { // Base case
        // We only admit to being the node to remove if the caller is
        // approaching us from level 0. It is entirely likely that
        // the same (or an other) caller can see us at a higher level
        // but the recursion stack will not have been set up in the correct
        // step wise fashion so that the lower level references will
        // not be swapped.
        // Effectively: if (call_level == 0 && value == _value) {
        if (call_level == 0 && !_compare(value, _value) && !_compare(_value, value)) {
            _nodeRefs.resetSwapLevel();
            return this;
        }
    }
    assert(pNode == nullptr);
    return nullptr;
}

/*
 * This checks the internal consistency of a Node. It returns 0
 * if successful, non-zero on error. The tests are:
 *
 * - Height must be >= 1
 * - Height must not exceed HeadNode height.
 * - NULL pointer must not have a non-NULL above them.
 * - Node pointers must not be self-referential.
 */
/**
 * This checks the internal consistency of a Node. It returns 0
 * if successful, non-zero on error. The tests are:
 *
 * - Height must be >= 1
 * - Height must not exceed HeadNode height.
 * - NULL pointer must not have a non-NULL above them.
 * - Node pointers must not be self-referential.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param headnode_height Height of HeadNode.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck Node<T, _Compare>::lacksIntegrity(size_t headnode_height) const {
    IntegrityCheck result = _nodeRefs.lacksIntegrity();
    if (result) {
        return result;
    }
    if (_nodeRefs.height() == 0) {
        return NODE_HEIGHT_ZERO;
    }
    if (_nodeRefs.height() > headnode_height) {
        return NODE_HEIGHT_EXCEEDS_HEADNODE;
    }
    // Test: All nodes above a nullprt must be nullptr
    size_t level = 0;
    while (level < _nodeRefs.height()) {
        if (! _nodeRefs[level].pNode) {
            break;
        }
        ++level;
    }
    while (level < _nodeRefs.height()) {
        if (_nodeRefs[level].pNode) {
            return NODE_NON_NULL_AFTER_NULL;
        }
        ++level;
    }
    // No reference should be to self.
    if (! _nodeRefs.noNodePointerMatches(this)) {
        return NODE_SELF_REFERENCE;
    }
    return INTEGRITY_SUCCESS;
}

/**
 * Checks that this Node is in the set held by the HeadNode.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param nodeSet Set of Nodes held by the HeadNode.
 * @return An IntegrityCheck enum.
 */
template <typename T, typename _Compare>
IntegrityCheck Node<T, _Compare>::lacksIntegrityRefsInSet(const std::set<const Node<T, _Compare>*> &nodeSet) const {
    size_t level = 0;
    while (level < _nodeRefs.height()) {
        if (nodeSet.count(_nodeRefs[level].pNode) == 0) {
            return NODE_REFERENCES_NOT_IN_GLOBAL_SET;
        }
        ++level;
    }
    return INTEGRITY_SUCCESS;
}

/**
 * Returns an estimate of the memory usage of an instance.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @return The memory estimate of this Node.
 */
template <typename T, typename _Compare>
size_t Node<T, _Compare>::size_of() const {
    // sizeof(*this) includes the size of _nodeRefs but _nodeRefs.size_of()
    // includes sizeof(_nodeRefs) so we need to subtract to avoid double counting
    return sizeof(*this) + _nodeRefs.size_of() - sizeof(_nodeRefs);
}


#ifdef INCLUDE_METHODS_THAT_USE_STREAMS

/**
 * Writes out this Node address.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param os Where to write.
 * @param suffix The suffix (node number).
 */
template <typename T, typename _Compare>
void Node<T, _Compare>::writeNode(std::ostream &os, size_t suffix) const {
    os << "\"node";
    os << suffix;
    os << std::hex << this << std::dec << "\"";
}

/**
 * Writes out a fragment of a DOT file representing this Node.
 *
 * @tparam T The type of the Skip List Node values.
 * @tparam _Compare A comparison function for type T.
 * @param os Wheere to write.
 * @param suffix The node number.
 */
template <typename T, typename _Compare>
void Node<T, _Compare>::dotFile(std::ostream &os, size_t suffix) const {
    assert(_nodeRefs.height());
    writeNode(os, suffix);
    os << " [" << std::endl;
    os << "label = \"";
    for (size_t level = _nodeRefs.height(); level-- > 0;) {
        os << " { <w" << level + 1 << "> " << _nodeRefs[level].width;
        os << " | <f" << level + 1 << "> ";
        os << std::hex << _nodeRefs[level].pNode << std::dec;
        os << " }";
        os << " |";
    }
    os << " <f0> " << _value << "\"" << std::endl;
    os << "shape = \"record\"" << std::endl;
    os << "];" << std::endl;
    // Now edges
    for (size_t level = 0; level < _nodeRefs.height(); ++level) {
        writeNode(os, suffix);
        os << ":f" << level + 1 << " -> ";
        _nodeRefs[level].pNode->writeNode(os, suffix);
        //        writeNode(os, suffix);
        //        os << ":f" << i + 1 << " [];" << std::endl;
        os << ":w" << level + 1 << " [];" << std::endl;
    }
    assert(_nodeRefs.height());
    if (_nodeRefs[0].pNode) {
        _nodeRefs[0].pNode->dotFile(os, suffix);
    }
}

#endif // INCLUDE_METHODS_THAT_USE_STREAMS

/************************** END: Node *******************************/

#endif // SkipList_Node_h
