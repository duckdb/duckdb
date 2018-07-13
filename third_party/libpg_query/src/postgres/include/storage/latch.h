/*-------------------------------------------------------------------------
 *
 * latch.h
 *	  Routines for interprocess latches
 *
 * A latch is a boolean variable, with operations that let processes sleep
 * until it is set. A latch can be set from another process, or a signal
 * handler within the same process.
 *
 * The latch interface is a reliable replacement for the common pattern of
 * using pg_usleep() or select() to wait until a signal arrives, where the
 * signal handler sets a flag variable. Because on some platforms an
 * incoming signal doesn't interrupt sleep, and even on platforms where it
 * does there is a race condition if the signal arrives just before
 * entering the sleep, the common pattern must periodically wake up and
 * poll the flag variable. The pselect() system call was invented to solve
 * this problem, but it is not portable enough. Latches are designed to
 * overcome these limitations, allowing you to sleep without polling and
 * ensuring quick response to signals from other processes.
 *
 * There are two kinds of latches: local and shared. A local latch is
 * initialized by InitLatch, and can only be set from the same process.
 * A local latch can be used to wait for a signal to arrive, by calling
 * SetLatch in the signal handler. A shared latch resides in shared memory,
 * and must be initialized at postmaster startup by InitSharedLatch. Before
 * a shared latch can be waited on, it must be associated with a process
 * with OwnLatch. Only the process owning the latch can wait on it, but any
 * process can set it.
 *
 * There are three basic operations on a latch:
 *
 * SetLatch		- Sets the latch
 * ResetLatch	- Clears the latch, allowing it to be set again
 * WaitLatch	- Waits for the latch to become set
 *
 * WaitLatch includes a provision for timeouts (which should be avoided
 * when possible, as they incur extra overhead) and a provision for
 * postmaster child processes to wake up immediately on postmaster death.
 * See unix_latch.c for detailed specifications for the exported functions.
 *
 * The correct pattern to wait for event(s) is:
 *
 * for (;;)
 * {
 *	   ResetLatch();
 *	   if (work to do)
 *		   Do Stuff();
 *	   WaitLatch();
 * }
 *
 * It's important to reset the latch *before* checking if there's work to
 * do. Otherwise, if someone sets the latch between the check and the
 * ResetLatch call, you will miss it and Wait will incorrectly block.
 *
 * To wake up the waiter, you must first set a global flag or something
 * else that the wait loop tests in the "if (work to do)" part, and call
 * SetLatch *after* that. SetLatch is designed to return quickly if the
 * latch is already set.
 *
 * On some platforms, signals will not interrupt the latch wait primitive
 * by themselves.  Therefore, it is critical that any signal handler that
 * is meant to terminate a WaitLatch wait calls SetLatch.
 *
 * Note that use of the process latch (PGPROC.procLatch) is generally better
 * than an ad-hoc shared latch for signaling auxiliary processes.  This is
 * because generic signal handlers will call SetLatch on the process latch
 * only, so using any latch other than the process latch effectively precludes
 * use of any generic handler.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/latch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LATCH_H
#define LATCH_H

#include <signal.h>

/*
 * Latch structure should be treated as opaque and only accessed through
 * the public functions. It is defined here to allow embedding Latches as
 * part of bigger structs.
 */
typedef struct Latch
{
	sig_atomic_t is_set;
	bool		is_shared;
	int			owner_pid;
#ifdef WIN32
	HANDLE		event;
#endif
} Latch;

/* Bitmasks for events that may wake-up WaitLatch() clients */
#define WL_LATCH_SET		 (1 << 0)
#define WL_SOCKET_READABLE	 (1 << 1)
#define WL_SOCKET_WRITEABLE  (1 << 2)
#define WL_TIMEOUT			 (1 << 3)
#define WL_POSTMASTER_DEATH  (1 << 4)

/*
 * prototypes for functions in latch.c
 */
extern void InitializeLatchSupport(void);
extern void InitLatch(volatile Latch *latch);
extern void InitSharedLatch(volatile Latch *latch);
extern void OwnLatch(volatile Latch *latch);
extern void DisownLatch(volatile Latch *latch);
extern int	WaitLatch(volatile Latch *latch, int wakeEvents, long timeout);
extern int WaitLatchOrSocket(volatile Latch *latch, int wakeEvents,
				  pgsocket sock, long timeout);
extern void SetLatch(volatile Latch *latch);
extern void ResetLatch(volatile Latch *latch);

/* beware of memory ordering issues if you use this macro! */
#define TestLatch(latch) (((volatile Latch *) (latch))->is_set)

/*
 * Unix implementation uses SIGUSR1 for inter-process signaling.
 * Win32 doesn't need this.
 */
#ifndef WIN32
extern void latch_sigusr1_handler(void);
#else
#define latch_sigusr1_handler()  ((void) 0)
#endif

#endif   /* LATCH_H */
