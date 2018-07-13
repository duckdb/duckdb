/*-------------------------------------------------------------------------
 *
 * dsm.h
 *	  manage dynamic shared memory segments
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/dsm.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DSM_H
#define DSM_H

#include "storage/dsm_impl.h"

typedef struct dsm_segment dsm_segment;

#define DSM_CREATE_NULL_IF_MAXSEGMENTS			0x0001

/* Startup and shutdown functions. */
struct PGShmemHeader;			/* avoid including pg_shmem.h */
extern void dsm_cleanup_using_control_segment(dsm_handle old_control_handle);
extern void dsm_postmaster_startup(struct PGShmemHeader *);
extern void dsm_backend_shutdown(void);
extern void dsm_detach_all(void);

#ifdef EXEC_BACKEND
extern void dsm_set_control_handle(dsm_handle h);
#endif

/* Functions that create, update, or remove mappings. */
extern dsm_segment *dsm_create(Size size, int flags);
extern dsm_segment *dsm_attach(dsm_handle h);
extern void *dsm_resize(dsm_segment *seg, Size size);
extern void *dsm_remap(dsm_segment *seg);
extern void dsm_detach(dsm_segment *seg);

/* Resource management functions. */
extern void dsm_pin_mapping(dsm_segment *seg);
extern void dsm_unpin_mapping(dsm_segment *seg);
extern void dsm_pin_segment(dsm_segment *seg);
extern dsm_segment *dsm_find_mapping(dsm_handle h);

/* Informational functions. */
extern void *dsm_segment_address(dsm_segment *seg);
extern Size dsm_segment_map_length(dsm_segment *seg);
extern dsm_handle dsm_segment_handle(dsm_segment *seg);

/* Cleanup hooks. */
typedef void (*on_dsm_detach_callback) (dsm_segment *, Datum arg);
extern void on_dsm_detach(dsm_segment *seg,
			  on_dsm_detach_callback function, Datum arg);
extern void cancel_on_dsm_detach(dsm_segment *seg,
					 on_dsm_detach_callback function, Datum arg);
extern void reset_on_dsm_detach(void);

#endif   /* DSM_H */
