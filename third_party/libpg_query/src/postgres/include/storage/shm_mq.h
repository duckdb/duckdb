/*-------------------------------------------------------------------------
 *
 * shm_mq.h
 *	  single-reader, single-writer shared memory message queue
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/shm_mq.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SHM_MQ_H
#define SHM_MQ_H

#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/proc.h"

/* The queue itself, in shared memory. */
struct shm_mq;
typedef struct shm_mq shm_mq;

/* Backend-private state. */
struct shm_mq_handle;
typedef struct shm_mq_handle shm_mq_handle;

/* Descriptors for a single write spanning multiple locations. */
typedef struct
{
	const char *data;
	Size		len;
} shm_mq_iovec;

/* Possible results of a send or receive operation. */
typedef enum
{
	SHM_MQ_SUCCESS,				/* Sent or received a message. */
	SHM_MQ_WOULD_BLOCK,			/* Not completed; retry later. */
	SHM_MQ_DETACHED				/* Other process has detached queue. */
} shm_mq_result;

/*
 * Primitives to create a queue and set the sender and receiver.
 *
 * Both the sender and the receiver must be set before any messages are read
 * or written, but they need not be set by the same process.  Each must be
 * set exactly once.
 */
extern shm_mq *shm_mq_create(void *address, Size size);
extern void shm_mq_set_receiver(shm_mq *mq, PGPROC *);
extern void shm_mq_set_sender(shm_mq *mq, PGPROC *);

/* Accessor methods for sender and receiver. */
extern PGPROC *shm_mq_get_receiver(shm_mq *);
extern PGPROC *shm_mq_get_sender(shm_mq *);

/* Set up backend-local queue state. */
extern shm_mq_handle *shm_mq_attach(shm_mq *mq, dsm_segment *seg,
			  BackgroundWorkerHandle *handle);

/* Associate worker handle with shm_mq. */
extern void shm_mq_set_handle(shm_mq_handle *, BackgroundWorkerHandle *);

/* Break connection. */
extern void shm_mq_detach(shm_mq *);

/* Get the shm_mq from handle. */
extern shm_mq *shm_mq_get_queue(shm_mq_handle *mqh);

/* Send or receive messages. */
extern shm_mq_result shm_mq_send(shm_mq_handle *mqh,
			Size nbytes, const void *data, bool nowait);
extern shm_mq_result shm_mq_sendv(shm_mq_handle *mqh,
			 shm_mq_iovec *iov, int iovcnt, bool nowait);
extern shm_mq_result shm_mq_receive(shm_mq_handle *mqh,
			   Size *nbytesp, void **datap, bool nowait);

/* Wait for our counterparty to attach to the queue. */
extern shm_mq_result shm_mq_wait_for_attach(shm_mq_handle *mqh);

/* Smallest possible queue. */
extern PGDLLIMPORT const Size shm_mq_minimum_size;

#endif   /* SHM_MQ_H */
