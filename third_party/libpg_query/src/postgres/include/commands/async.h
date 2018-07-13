/*-------------------------------------------------------------------------
 *
 * async.h
 *	  Asynchronous notification: NOTIFY, LISTEN, UNLISTEN
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/async.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ASYNC_H
#define ASYNC_H

#include <signal.h>

#include "fmgr.h"

/*
 * The number of SLRU page buffers we use for the notification queue.
 */
#define NUM_ASYNC_BUFFERS	8

extern bool Trace_notify;
extern volatile sig_atomic_t notifyInterruptPending;

extern Size AsyncShmemSize(void);
extern void AsyncShmemInit(void);

/* notify-related SQL statements */
extern void Async_Notify(const char *channel, const char *payload);
extern void Async_Listen(const char *channel);
extern void Async_Unlisten(const char *channel);
extern void Async_UnlistenAll(void);

/* notify-related SQL functions */
extern Datum pg_listening_channels(PG_FUNCTION_ARGS);
extern Datum pg_notify(PG_FUNCTION_ARGS);

/* perform (or cancel) outbound notify processing at transaction commit */
extern void PreCommit_Notify(void);
extern void AtCommit_Notify(void);
extern void AtAbort_Notify(void);
extern void AtSubStart_Notify(void);
extern void AtSubCommit_Notify(void);
extern void AtSubAbort_Notify(void);
extern void AtPrepare_Notify(void);
extern void ProcessCompletedNotifies(void);

/* signal handler for inbound notifies (PROCSIG_NOTIFY_INTERRUPT) */
extern void HandleNotifyInterrupt(void);

/* process interrupts */
extern void ProcessNotifyInterrupt(void);

#endif   /* ASYNC_H */
