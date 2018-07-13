/*-------------------------------------------------------------------------
 *
 * shm_toc.h
 *	  shared memory segment table of contents
 *
 * This is intended to provide a simple way to divide a chunk of shared
 * memory (probably dynamic shared memory allocated via dsm_create) into
 * a number of regions and keep track of the addreses of those regions or
 * key data structures within those regions.  This is not intended to
 * scale to a large number of keys and will perform poorly if used that
 * way; if you need a large number of pointers, store them within some
 * other data structure within the segment and only put the pointer to
 * the data structure itself in the table of contents.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shm_toc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHM_TOC_H
#define SHM_TOC_H

#include "storage/shmem.h"

struct shm_toc;
typedef struct shm_toc shm_toc;

extern shm_toc *shm_toc_create(uint64 magic, void *address, Size nbytes);
extern shm_toc *shm_toc_attach(uint64 magic, void *address);
extern void *shm_toc_allocate(shm_toc *toc, Size nbytes);
extern Size shm_toc_freespace(shm_toc *toc);
extern void shm_toc_insert(shm_toc *toc, uint64 key, void *address);
extern void *shm_toc_lookup(shm_toc *toc, uint64 key);

/*
 * Tools for estimating how large a chunk of shared memory will be needed
 * to store a TOC and its dependent objects.
 */
typedef struct
{
	Size		space_for_chunks;
	Size		number_of_keys;
} shm_toc_estimator;

#define shm_toc_initialize_estimator(e) \
	((e)->space_for_chunks = 0, (e)->number_of_keys = 0)
#define shm_toc_estimate_chunk(e, sz) \
	((e)->space_for_chunks = add_size((e)->space_for_chunks, \
		BUFFERALIGN((sz))))
#define shm_toc_estimate_keys(e, cnt) \
	((e)->number_of_keys = add_size((e)->number_of_keys, (cnt)))

extern Size shm_toc_estimate(shm_toc_estimator *);

#endif   /* SHM_TOC_H */
