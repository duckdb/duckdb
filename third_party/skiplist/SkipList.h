#ifndef __SkipList__SkipList__
#define __SkipList__SkipList__

/**
 * @file
 *
 * Project: skiplist
 *
 * Created by Paul Ross on 15/11/2015.
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

/** @mainpage
 *
 * General
 * =======
 * This is a generic skip list implementation for any type T.
 * There only restriction on the size of this skip list is the available memory.
 *
 * A skip list is a singly linked list of ordered nodes with a series of other, coarser, lists that reference a subset
 * of nodes in order.
 * 'Level' is an size_t that specifies the coarseness of the linked list, level 0 is the linked list to every node.
 *
 * Typically:
 * - The list at level 1 links (ideally) to every other node.
 * - The list at level 2 links (ideally) to every fourth node and so on.
 *
 * In general the list at level n links (ideally) to every 2**n node.
 *
 * These additional lists allow rapid location, insertion and removal of nodes.
 * These lists are created and updated in a probabilistic manner and this is achieved at node creation time by tossing a
 * virtual coin.
 * These lists are not explicit, they are implied by the references between Nodes at a particular level.
 *
 * Skip lists are alternatives to balanced trees for operations such as a rolling median.
 * The disadvantages of skip lists are:
    - Less space efficient than balanced trees (see 'Space Complexity' below).
    - performance is similar to balanced trees except finding the mid-point which is @c O(log(N)) for a skip list
        compared with @c O(1) for a balanced tree.
 *
 * The advantages claimed for skip lists are:
    - The insert() and remove() logic is simpler (I do not subscribe to this).
 *
 * Examples of Usage
 * =================
 *
 * C++
 * ---
 * @code
 * #include "SkipList.h"
 *
 * OrderedStructs::SkipList::HeadNode<double> sl;
 *
 * sl.insert(42.0);
 * sl.insert(21.0);
 * sl.insert(84.0);
 * sl.has(42.0) // true
 * sl.size() // 3
 * sl.at(1) // 42.0
 * @endcode
 *
 * Python
 * ------
 * @code
 * import orderedstructs
 *
 * sl = orderedstructs.SkipList(float)
 * sl.insert(42.0)
 * sl.insert(21.0)
 * sl.insert(84.0)
 * sl.has(42.0) # True
 * sl.size() # 3
 * sl.at(1) # 42.0
 * @endcode
 *
 * Design
 * ======
 *
 * This skip list design has the coarser lists implemented as optional additional links between the nodes themselves.
 * The drawing below shows a well formed skip list with a head node ('HED') linked to the ordered nodes A to H.
 *
 * @code
 *
 | 5 E |------------------------------------->| 4 0 |---------------------------->| NULL |
 | 1 A |->| 2 C |---------->| 2 E |---------->| 2 G |---------->| 2 0 |---------->| NULL |
 | 1 A |->| 1 B |->| 1 C |->| 1 D |->| 1 E |->| 1 F |->| 1 G |->| 1 H |->| 1 0 |->| NULL |
 | HED |  |  A  |  |  B  |  |  C  |  |  D  |  |  E  |  |  F  |  |  G  |  |  H  |
 * @endcode
 *
 * Each node has a stack of values that consist of a 'width' and a reference to another node (or NULL).
 * At the lowest level is a singly linked list and all widths are 1.
 * At level 1 the links are (ideally) to every other node and at level 2 the links are (ideally) to every fourth node.
 * The 'widths' at each node/level specify how many level 0 nodes the node reference skips over.
 * The widths are used to rapidly index into the skip list starting from the highest level and working down.
 *
 * To understand how the skip list is maintained, consider insertion; before inserting node 'E' the skip list would look
 * like this:
 *
 * @code
 *
 | 1 A |->| 2 C |---------->| 3 G |------------------->| 2 0 |---------->| NULL |
 | 1 A |->| 1 B |->| 1 C |->| 1 D |->| 1 F |->| 1 G |->| 1 H |->| 1 0 |->| NULL |
 | HED |  |  A  |  |  B  |  |  C  |  |  D  |  |  F  |  |  G  |  |  H  |
 *
 * @endcode
 *
 * Inserting 'E' means:
 * - Finding where 'E' should be inserted (after 'D').
 * - Creating node 'E' with a random height (heads/heads/tails so 3 high).
 * - Updating 'D' to refer to 'E' at level 0.
 * - Updating 'C' to refer to 'E' at level 1 and decreasing C's width to 2, increasing 'E' width at level 1 to 2.
 * - Expanding HED to level 2 with a reference to 'E' and a width of 5.
 * - Updating 'E' with a reference to NULL and a width of 4.
 *
 * Recursive Search for the Node Position
 * --------------------------------------
 * The first two operations are done by a recursive search.
 * This creates the chain HED[1], A[1], C[1], C[0], D[0] thus E will be created at level 0 and inserted after D.
 *
 * Node Creation
 * -------------
 * Node E is created with a stack containing a single pointer to the next node F.
 * Then a virtual coin is tossed, for each 'head' and extra NULL reference is added to the stack.
 * If a 'tail' is thrown the stack is complete.
 * In the example above when creating Node E we have encountered tosses of 'head', 'head', 'tail'.
 *
 * Recursive Unwinding
 * -------------------
 * The remaining operations are done as recursion unwinds:
 *
 * - D[0] and C[0] update E[1] with their cumulative width (2).
 * - C[1] adds 1 to width (a new node is inserted) then subtracts E[1].
 * - Then C[1]/E[1] are swapped so that the pointers and widths are correct.
 * - And so on until HED is reached, in this case a new level is added and HED[2] swapped with E[2].
 *
 * A similar procedure will be followed, in reverse, when removing E to restore the state of the skip list to the
 * picture above.
 *
 * Algorithms
 * ==========
 * There doesn't seem to be much literature that I could find about the algorithms used for a skip list so these have
 * all been invented here.
 *
 * In these descriptions:
 *
 * - 'right' is used to mean move to a higher ordinal node.
 * - 'left' means to move to a lower ordinal node.
 * - 'up' means to move to a coarser grained list, 'top' is the highest.
 * - 'down' means to move to a finer grained list, 'bottom' is the level 0.
 *
 * has(T &val) const;
 * ------------------
 * This returns true/false is the skip list has the value val.
 * Starting at the highest possible level search rightwards until a larger value is encountered, then drop down.
 * At level 0 return true if the Node value is the supplied value.
 * This is @c O(log(N)) for well formed skip lists.
 *
 * at(size_t index) const;
 * -----------------------
 * This returns the value of type T at the given index.
 * The algorithm is similar to has(T &val) but the search moves rightwards if the width is less than the index and
 * decrementing the index by the width.
 *
 * If progress can not be made to the right, drop down a level.
 * If the index is 0 return the node value.
 * This is @c O(log(N)) for well formed skip lists.
 *
 * insert(T &val)
 * --------------
 * Finding the place to insert a node follows the has(T &val) algorithm to find the place in the skip list to create a
 * new node.
 * A duplicate value is inserted after any existing duplicate values.
 *
 * - All nodes are inserted at level 0 even if the insertion point can be seen at a higher level.
 * - The search for an insertion location creates a recursion stack that, when unwound, updates the traversed nodes
 *      <tt>{width, Node<T>*}</tt> data.
 * - Once an insert position is found a Node is created whose height is determined by repeatedly tossing a virtual coin
 *      until a 'tails' is thrown.
 * - This node initially has all node references to be to itself (this), and the widths set to 1 for level 0 and 0 for
 *      the remaining levels, they will be used to sum the widths at one level down.
 * - On recursion ('left') each node adds its width to the new node at the level above the current level.
 * - On moving up a level the current node swaps its width and node pointer with the new node at that new level.
 *
 * remove(T &val)
 * --------------
 *
 * If there are duplicate values the last one is removed first, this is for symmetry with insert().
 * Essentially this is the same as insert() but once the node is found the insert() updating algorithm is reversed and
 * the node deleted.
 *
 * Code Layout
 * ===========
 * There are three classes defined in their own .h files and these are all included into the SkipList.h file.
 *
 * The classes are:
 *
 * <tt>SwappableNodeRefStack</tt>
 *
 * This is simple bookkeeping class that has a vector of <tt>[{skip_width, Node<T>*}, ...]</tt>.
 * This vector can be expanded or contracted at will.
 * Both HeadNode and Node classes have one of these to manage their references.
 *
 * <tt>Node</tt>
 *
 * This represents a single value in the skip list.
 * The height of a Node is determined at construction by tossing a virtual coin, this determines how many coarser
 * lists this node participates in.
 * A Node has a SwappableNodeRefStack object and a value of type T.
 *
 * <tt>HeadNode</tt>
 *
 * There is one of these per skip list and this provides the API to the entire skip list.
 * The height of the HeadNode expands and contracts as required when Nodes are inserted or removed (it is the height
 * of the highest Node).
 * A HeadNode has a SwappableNodeRefStack object and an independently maintained count of the number of Node objects
 * in the skip list.
 *
 * A Node and HeadNode have specialised methods such as has(), at(), insert(), remove() that traverse the skip lis
 * recursively.
 *
 * Other Files of Significance
 * ---------------------------
 * SkipList.cpp exposes the random number generator (rand()) and seeder (srand()) so that they can be accessed
 * CPython for deterministic testing.
 *
 * cSkipList.h and cSkipList.cpp contains a CPython module with a SkipList implementation for a number of builtin
 * Python types.
 *
 * IntegrityEnums.h has definitions of error codes that can be created by the skip list integrity checking functions.
 *
 * Code Idioms
 * ===========
 *
 * Prevent Copying
 * ---------------
 * Copying operations are (mostly) prohibited for performance reasons.
 * The only class that allows copying is struct NodeRef that contains fundamental types.
 * All other classes declare their copying operation private and unimplemented (rather than using C++11 delete) for
 * compatibility with older compilers.
 *
 * Reverse Loop of Unsigned int
 * ----------------------------
 * In a lot of the code we have to count down from some value to 0
 * with a size_t (an unsigned integer type) The idiom used is this:
 *
 * @code
 *
 *  for (size_t l = height(); l-- > 0;) {
 *      // ...
 *  }
 *
 * @endcode
 *
 * The "l-- > 0" means test l against 0 then decrement it.
 * l will thus start at the value height() - 1 down to 0 then exit the loop.
 *
 * @note If l is declared before the loop it will have the maximum value of a size_t unless a break statement is
 * encountered.
 *
 * Roads not Travelled
 * ===================
 * Certain designs were not explored, here they are and why.
 *
 * Key/Value Implementation
 * ------------------------
 * Skip lists are commonly used for key/value dictionaries. Given things
 * like map<T> or unorderedmap<T> I see no reason why a SkipList should be used
 * as an alternative.
 *
 * Adversarial Users
 * -----------------
 * If the user knows the behaviour of the random number generator it is possible that they can change the order of
 * insertion to create a poor distribution of nodes which will make operations tend to O(N) rather than O(log(N)).
 *
 * Probability != 0.5
 * ------------------
 * This implementation uses a fair coin to decide the height of the node.
 *
 * Some literature suggests other values such as p = 0.25 might be more efficient.
 * Some experiments seem to show that this is the case with this implementation.
 * Here are some results when using a vector of 1 million doubles and a sliding window of 101 where each value is
 * inserted and removed and the cental value recovered:
 *
 * @code
 *
    Probability calculation        p    Time compared to p = 0.5
     rand() < RAND_MAX / 16;    0.0625   90%
     rand() < RAND_MAX / 8;     0.125    83%
     rand() < RAND_MAX / 4;     0.25     80%
     rand() < RAND_MAX / 2;     0.5     100%
     rand() > RAND_MAX / 4;     0.75    143%
     rand() > RAND_MAX / 8;     0.875   201%
 *
 * @endcode
 *
 * Optimisation: Re-index Nodes on Complete Traversal
 * --------------------------------------------------
 *
 * @todo Re-index Nodes on Complete Traversal ???
 *
 * Optimisation: Reuse removed nodes for insert()
 * ----------------------------------------------
 * @todo Reuse removed nodes for insert() ???
 *
 * Reference Counting
 * ------------------
 * Some time (and particularly space) improvement could be obtained by reference counting nodes so that duplicate
 * values could be eliminated.
 * Since the primary use case for this skip list is for computing the rolling median of doubles the chances of
 * duplicates are slim.
 * For int, long and string there is a higher probability so reference counting might be implemented in the future if
 * these types become commonly used.
 *
 * Use and Array of <tt>{skip_width, Node<T>*}</tt> rather than a vector
 * ----------------------------------------------------------------------
 *
 * Less space would be used for each Node if the SwappableNodeRefStack used a dynamically allocated array of
 * <tt>[{skip_width, Node<T>*}, ...]</tt> rather than a vector.
 *
 * Performance
 * ===========
 *
 * Reference platform: Macbook Pro, 13" running OS X 10.9.5. LLVM version 6.0 targeting x86_64-apple-darwin13.4.0
 * Compiled with -Os (small fast).
 *
 * Performance of at() and has()
 * -----------------------------
 *
 * Performance is O(log(N)) where N is the position in the skip list.
 *
 * On the reference platform this tests as t = 200 log2(N) in nanoseconds for skip lists of doubles.
 * This factor of 200 can be between 70 and 500 for the same data but different indices because of the probabilistic
 * nature of a skip list.
 * For example finding the mid value of 1M doubles takes 3 to 4 microseconds.
 *
 * @note
 * On Linux RHEL5 with -O3 this is much faster with t = 12 log2(N)
 * [main.cpp perf_at_in_one_million(), main.cpp perf_has_in_one_million()]
 *
 * Performance of insert() and remove()
 * ------------------------------------
 * A test that inserts then removes a single value in an empty list takes 440 nanoseconds (around 2.3 million per
 * second).
 * This should be fast as the search space is small.
 *
 * @note
 * Linux RHEL5 with -O3 this is 4.2 million per second. [main.cpp perf_single_insert_remove()]
 *
 * A test that inserts 1M doubles into a skip list (no removal) takes 0.9 seconds (around 1.1 million per second).
 *
 * @note
 * Linux RHEL5 with -O3 this is similar. [main.cpp perf_large_skiplist_ins_only()]
 *
 * A test that inserts 1M doubles into a skip list then removes all of them takes 1.0 seconds (around 1 million per second).
 *
 * @note
 * Linux RHEL5 with -O3 this is similar. [main.cpp perf_large_skiplist_ins_rem()]
 *
 * A test that creates a skip list of 1M doubles then times how long it takes to insert and remove a value at the
 * mid-point takes 1.03 microseconds per item (around 1 million per second).
 *
 * @note
 * Linux RHEL5 with -O3 this is around 0.8 million per second. [main.cpp perf_single_ins_rem_middle()]
 *
 * A test that creates a skip list of 1M doubles then times how long it takes to insert a value, find the value at the
 * mid point then remove that value (using insert()/at()/remove()) takes 1.2 microseconds per item (around 0.84 million
 * per second).
 *
 * @note
 * Linux RHEL5 with -O3 this is around 0.7 million per second. [main.cpp perf_single_ins_at_rem_middle()]
 *
 * Performance of a rolling median
 * -------------------------------
 * On the reference platform a rolling median (using insert()/at()/remove()) on 1M random values takes about 0.93
 * seconds.
 *
 * @note
 * Linux RHEL5 with -O3 this is about 0.7 seconds.
 * [main.cpp perf_1m_median_values(), main.cpp perf_1m_medians_1000_vectors(), main.cpp perf_simulate_real_use()]
 *
 * The window size makes little difference, a rolling median on 1m items with a window size of 1 takes 0.491 seconds,
 * with a window size of 524288 it takes 1.03 seconds.
 *
 * @note
 * Linux RHEL5 with -O3 this is about 0.5 seconds. [main.cpp perf_roll_med_odd_index_wins()]
 *
 * Space Complexity
 * ----------------
 * Given:
 *
 * - t = sizeof(T) ~ typ. 8 bytes for a double
 * - v = sizeof(std::vector<struct NodeRef<T>>) ~ typ. 32 bytes
 * - p = sizeof(Node<T>*) ~ typ. 8 bytes
 * - e = sizeof(struct NodeRef<T>) ~ typ. 8 + p = 16 bytes
 *
 * Then each node: is t + v bytes.
 *
 * Linked list at level 0 is e bytes per node.
 *
 * Linked list at level 1 is, typically, e / 2 bytes per node and so on.
 *
 * So the totality of linked lists is about 2e bytes per node.
 *
 * The total is N * (t + v + 2 * e) which for T as a double is typically 72 bytes per item.
 *
 * In practice this has been measured on the reference platform as a bit larger at 86.0 Mb for 1024*1024 doubles.
 *
 ***************** END: SkipList Documentation *****************/

/// Defined if you want the SkipList to have methods that can output
/// to stream (for debugging for example).
/// Defining this will mean that classes grow methods that use streams.
/// Undef this if you want a smaller binary in production as using streams
/// adds typically around 30kb to the binary.
/// However you may loose useful information such as formatted
/// exception messages with extra data.
//#define INCLUDE_METHODS_THAT_USE_STREAMS
#undef INCLUDE_METHODS_THAT_USE_STREAMS

#include <functional>
#include <vector>
#include <set> // Used for HeadNode::_lacksIntegrityNodeReferencesNotInList()
#include <string> // Used for class Exception
#include <random>
#include "pcg_random.hpp"

#ifdef DEBUG
#include <cassert>
#else
#ifndef assert
#define assert(x)
#endif
#endif // DEBUG

#ifdef INCLUDE_METHODS_THAT_USE_STREAMS

#include <iostream>
#include <sstream>

#endif // INCLUDE_METHODS_THAT_USE_STREAMS

//#define SKIPLIST_THREAD_SUPPORT
//#define SKIPLIST_THREAD_SUPPORT_TRACE

#ifdef SKIPLIST_THREAD_SUPPORT
#ifdef SKIPLIST_THREAD_SUPPORT_TRACE
#include <thread>
#endif
#include <mutex>
#endif

/**
 * @brief Namespace for all the C++ ordered structures.
 */
namespace duckdb_skiplistlib {
    /**
     * @brief Namespace for the C++ Slip List.
     */
    namespace skip_list {

/************************ Exceptions ****************************/

/**
 * @brief Base exception class for all exceptions in the OrderedStructs::SkipList namespace.
 */
        class Exception : public std::exception {
        public:
            explicit Exception(const std::string &in_msg) : msg(in_msg) {}

            const std::string &message() const { return msg; }

            virtual ~Exception() throw() {}

        protected:
            std::string msg;
        };

/**
 * @brief Specialised exception case for an index out of range error.
 */
        class IndexError : public Exception {
        public:
            explicit IndexError(const std::string &in_msg) : Exception(in_msg) {}
        };

/**
 * @brief Specialised exception for an value error where the given value does not exist in the Skip List.
 */
        class ValueError : public Exception {
        public:
            explicit ValueError(const std::string &in_msg) : Exception(in_msg) {}
        };

/** @brief Specialised exception used for NaN detection where value != value (example NaNs). */
        class FailedComparison : public Exception {
        public:
            explicit FailedComparison(const std::string &in_msg) : Exception(in_msg) {}
        };

/**
 * This throws an IndexError when the index value >= the size of Skip List.
 * If @ref INCLUDE_METHODS_THAT_USE_STREAMS is defined then the error will have an informative message.
 *
 * @param index The out of range index.
 */
        void _throw_exceeds_size(size_t index);

/************************ END: Exceptions ****************************/

#ifdef SKIPLIST_THREAD_SUPPORT
        /**
         * Mutex used in a multi-threaded environment.
         */
        extern std::mutex gSkipListMutex;
#endif

#include "NodeRefs.h"
#include "Node.h"
#include "HeadNode.h"

    } // namespace skip_list
} // namespace duckdb_skiplistlib

#endif /* defined(__SkipList__SkipList__) */
