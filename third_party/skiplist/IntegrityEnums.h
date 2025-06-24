#ifndef SkipList_IntegrityEnums_h
#define SkipList_IntegrityEnums_h

/**
 * @file
 *
 * Project: skiplist
 *
 * Integrity codes for structures in this code.
 *
 * Created by Paul Ross on 11/12/2015.
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

/**
 * Various integrity codes for structures in this code.
 */
enum IntegrityCheck {
    INTEGRITY_SUCCESS = 0,
    // SwappableNodeRefStack integrity checks
    NODEREFS_WIDTH_ZERO_NOT_UNITY = 100,
    NODEREFS_WIDTH_DECREASING,
    // Node integrity checks
    NODE_HEIGHT_ZERO = 200,
    NODE_HEIGHT_EXCEEDS_HEADNODE,
    NODE_NON_NULL_AFTER_NULL,
    NODE_SELF_REFERENCE,
    NODE_REFERENCES_NOT_IN_GLOBAL_SET,
    // HeadNode integrity checks
    HEADNODE_CONTAINS_NULL = 300,
    HEADNODE_COUNT_MISMATCH,
    HEADNODE_LEVEL_WIDTHS_MISMATCH,
    HEADNODE_DETECTS_CYCLIC_REFERENCE,
    HEADNODE_DETECTS_OUT_OF_ORDER,
};

#endif
