/*
 * xlogdefs.h
 *
 * Postgres transaction log manager record pointer and
 * timeline number definitions
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/xlogdefs.h
 */
#ifndef XLOG_DEFS_H
#define XLOG_DEFS_H

#include <fcntl.h>				/* need open() flags */

/*
 * Pointer to a location in the XLOG.  These pointers are 64 bits wide,
 * because we don't want them ever to overflow.
 */
typedef uint64 XLogRecPtr;

/*
 * Zero is used indicate an invalid pointer. Bootstrap skips the first possible
 * WAL segment, initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG
 * record can begin at zero.
 */
#define InvalidXLogRecPtr	0
#define XLogRecPtrIsInvalid(r)	((r) == InvalidXLogRecPtr)

/*
 * XLogSegNo - physical log file sequence number.
 */
typedef uint64 XLogSegNo;

/*
 * TimeLineID (TLI) - identifies different database histories to prevent
 * confusion after restoring a prior state of a database installation.
 * TLI does not change in a normal stop/restart of the database (including
 * crash-and-recover cases); but we must assign a new TLI after doing
 * a recovery to a prior state, a/k/a point-in-time recovery.  This makes
 * the new WAL logfile sequence we generate distinguishable from the
 * sequence that was generated in the previous incarnation.
 */
typedef uint32 TimeLineID;

/*
 * Replication origin id - this is located in this file to avoid having to
 * include origin.h in a bunch of xlog related places.
 */
typedef uint16 RepOriginId;

/*
 *	Because O_DIRECT bypasses the kernel buffers, and because we never
 *	read those buffers except during crash recovery or if wal_level != minimal,
 *	it is a win to use it in all cases where we sync on each write().  We could
 *	allow O_DIRECT with fsync(), but it is unclear if fsync() could process
 *	writes not buffered in the kernel.  Also, O_DIRECT is never enough to force
 *	data to the drives, it merely tries to bypass the kernel cache, so we still
 *	need O_SYNC/O_DSYNC.
 */
#ifdef O_DIRECT
#define PG_O_DIRECT				O_DIRECT
#else
#define PG_O_DIRECT				0
#endif

/*
 * This chunk of hackery attempts to determine which file sync methods
 * are available on the current platform, and to choose an appropriate
 * default method.  We assume that fsync() is always available, and that
 * configure determined whether fdatasync() is.
 */
#if defined(O_SYNC)
#define OPEN_SYNC_FLAG		O_SYNC
#elif defined(O_FSYNC)
#define OPEN_SYNC_FLAG		O_FSYNC
#endif

#if defined(O_DSYNC)
#if defined(OPEN_SYNC_FLAG)
/* O_DSYNC is distinct? */
#if O_DSYNC != OPEN_SYNC_FLAG
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#else							/* !defined(OPEN_SYNC_FLAG) */
/* Win32 only has O_DSYNC */
#define OPEN_DATASYNC_FLAG		O_DSYNC
#endif
#endif

#if defined(PLATFORM_DEFAULT_SYNC_METHOD)
#define DEFAULT_SYNC_METHOD		PLATFORM_DEFAULT_SYNC_METHOD
#elif defined(OPEN_DATASYNC_FLAG)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_OPEN_DSYNC
#elif defined(HAVE_FDATASYNC)
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FDATASYNC
#else
#define DEFAULT_SYNC_METHOD		SYNC_METHOD_FSYNC
#endif

#endif   /* XLOG_DEFS_H */
