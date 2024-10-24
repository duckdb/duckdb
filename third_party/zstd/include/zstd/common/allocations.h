/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

/* This file provides custom allocation primitives
 */
#ifndef ZSTD_ALLOCATIONS_H
#define ZSTD_ALLOCATIONS_H

#define ZSTD_DEPS_NEED_MALLOC
#include "zstd/common/zstd_deps.h"   /* ZSTD_malloc, ZSTD_calloc, ZSTD_free, ZSTD_memset */

#include "zstd/common/compiler.h" /* MEM_STATIC */
#define ZSTD_STATIC_LINKING_ONLY
#include "zstd.h" /* ZSTD_customMem */

namespace duckdb_zstd {

/* custom memory allocation functions */

MEM_STATIC void* ZSTD_customMalloc(size_t size, ZSTD_customMem customMem)
{
    if (customMem.customAlloc)
        return customMem.customAlloc(customMem.opaque, size);
    return ZSTD_malloc(size);
}

MEM_STATIC void* ZSTD_customCalloc(size_t size, ZSTD_customMem customMem)
{
    if (customMem.customAlloc) {
        /* calloc implemented as malloc+memset;
         * not as efficient as calloc, but next best guess for custom malloc */
        void* const ptr = customMem.customAlloc(customMem.opaque, size);
        ZSTD_memset(ptr, 0, size);
        return ptr;
    }
    return ZSTD_calloc(1, size);
}

MEM_STATIC void ZSTD_customFree(void* ptr, ZSTD_customMem customMem)
{
    if (ptr!=NULL) {
        if (customMem.customFree)
            customMem.customFree(customMem.opaque, ptr);
        else
            ZSTD_free(ptr);
    }
}

} // namespace duckdb_zstd

#endif /* ZSTD_ALLOCATIONS_H */
