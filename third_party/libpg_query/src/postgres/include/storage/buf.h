/*-------------------------------------------------------------------------
 *
 * buf.h
 *	  Basic buffer manager data types.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUF_H
#define BUF_H

/*
 * Buffer identifiers.
 *
 * Zero is invalid, positive is the index of a shared buffer (1..NBuffers),
 * negative is the index of a local buffer (-1 .. -NLocBuffer).
 */
typedef int Buffer;

#define InvalidBuffer	0

/*
 * BufferIsInvalid
 *		True iff the buffer is invalid.
 */
#define BufferIsInvalid(buffer) ((buffer) == InvalidBuffer)

/*
 * BufferIsLocal
 *		True iff the buffer is local (not visible to other backends).
 */
#define BufferIsLocal(buffer)	((buffer) < 0)

/*
 * Buffer access strategy objects.
 *
 * BufferAccessStrategyData is private to freelist.c
 */
typedef struct BufferAccessStrategyData *BufferAccessStrategy;

#endif   /* BUF_H */
