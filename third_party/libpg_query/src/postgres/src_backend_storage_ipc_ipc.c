/*--------------------------------------------------------------------
 * Symbols referenced in this file:
 * - proc_exit_inprogress
 * - proc_exit
 *--------------------------------------------------------------------
 */

/*-------------------------------------------------------------------------
 *
 * ipc.c
 *	  POSTGRES inter-process communication definitions.
 *
 * This file is misnamed, as it no longer has much of anything directly
 * to do with IPC.  The functionality here is concerned with managing
 * exit-time cleanup for either a postmaster or a backend.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/ipc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <sys/stat.h>

#include "miscadmin.h"
#ifdef PROFILE_PID_DIR
#include "postmaster/autovacuum.h"
#endif
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "tcop/tcopprot.h"


/*
 * This flag is set during proc_exit() to change ereport()'s behavior,
 * so that an ereport() from an on_proc_exit routine cannot get us out
 * of the exit procedure.  We do NOT want to go back to the idle loop...
 */
__thread bool		proc_exit_inprogress = false;


/*
 * This flag tracks whether we've called atexit() in the current process
 * (or in the parent postmaster).
 */


/* local functions */
static void proc_exit_prepare(int code);


/* ----------------------------------------------------------------
 *						exit() handling stuff
 *
 * These functions are in generally the same spirit as atexit(),
 * but provide some additional features we need --- in particular,
 * we want to register callbacks to invoke when we are disconnecting
 * from a broken shared-memory context but not exiting the postmaster.
 *
 * Callback functions can take zero, one, or two args: the first passed
 * arg is the integer exitcode, the second is the Datum supplied when
 * the callback was registered.
 * ----------------------------------------------------------------
 */

#define MAX_ON_EXITS 20

struct ONEXIT
{
	pg_on_exit_callback function;
	Datum		arg;
};










/* ----------------------------------------------------------------
 *		proc_exit
 *
 *		this function calls all the callbacks registered
 *		for it (to free resources) and then calls exit.
 *
 *		This should be the only function to call exit().
 *		-cim 2/6/90
 *
 *		Unfortunately, we can't really guarantee that add-on code
 *		obeys the rule of not calling exit() directly.  So, while
 *		this is the preferred way out of the system, we also register
 *		an atexit callback that will make sure cleanup happens.
 * ----------------------------------------------------------------
 */
void proc_exit(int code) { printf("Terminating process due to FATAL error\n"); exit(1); }


/*
 * Code shared between proc_exit and the atexit handler.  Note that in
 * normal exit through proc_exit, this will actually be called twice ...
 * but the second call will have nothing to do.
 */


/* ------------------
 * Run all of the on_shmem_exit routines --- but don't actually exit.
 * This is used by the postmaster to re-initialize shared memory and
 * semaphores after a backend dies horribly.  As with proc_exit(), we
 * remove each callback from the list before calling it, to avoid
 * infinite loop in case of error.
 * ------------------
 */


/* ----------------------------------------------------------------
 *		atexit_callback
 *
 *		Backstop to ensure that direct calls of exit() don't mess us up.
 *
 * Somebody who was being really uncooperative could call _exit(),
 * but for that case we have a "dead man switch" that will make the
 * postmaster treat it as a crash --- see pmsignal.c.
 * ----------------------------------------------------------------
 */


/* ----------------------------------------------------------------
 *		on_proc_exit
 *
 *		this function adds a callback function to the list of
 *		functions invoked by proc_exit().   -cim 2/6/90
 * ----------------------------------------------------------------
 */


/* ----------------------------------------------------------------
 *		before_shmem_exit
 *
 *		Register early callback to perform user-level cleanup,
 *		e.g. transaction abort, before we begin shutting down
 *		low-level subsystems.
 * ----------------------------------------------------------------
 */


/* ----------------------------------------------------------------
 *		on_shmem_exit
 *
 *		Register ordinary callback to perform low-level shutdown
 *		(e.g. releasing our PGPROC); run after before_shmem_exit
 *		callbacks and before on_proc_exit callbacks.
 * ----------------------------------------------------------------
 */


/* ----------------------------------------------------------------
 *		cancel_before_shmem_exit
 *
 *		this function removes a previously-registed before_shmem_exit
 *		callback.  For simplicity, only the latest entry can be
 *		removed.  (We could work harder but there is no need for
 *		current uses.)
 * ----------------------------------------------------------------
 */


/* ----------------------------------------------------------------
 *		on_exit_reset
 *
 *		this function clears all on_proc_exit() and on_shmem_exit()
 *		registered functions.  This is used just after forking a backend,
 *		so that the backend doesn't believe it should call the postmaster's
 *		on-exit routines when it exits...
 * ----------------------------------------------------------------
 */

