/*-------------------------------------------------------------------------
 *
 * dest.h
 *	  support for communication destinations
 *
 * Whenever the backend executes a query that returns tuples, the results
 * have to go someplace.  For example:
 *
 *	  - stdout is the destination only when we are running a
 *		standalone backend (no postmaster) and are returning results
 *		back to an interactive user.
 *
 *	  - a remote process is the destination when we are
 *		running a backend with a frontend and the frontend executes
 *		PQexec() or PQfn().  In this case, the results are sent
 *		to the frontend via the functions in backend/libpq.
 *
 *	  - DestNone is the destination when the system executes
 *		a query internally.  The results are discarded.
 *
 * dest.c defines three functions that implement destination management:
 *
 * BeginCommand: initialize the destination at start of command.
 * CreateDestReceiver: return a pointer to a struct of destination-specific
 * receiver functions.
 * EndCommand: clean up the destination at end of command.
 *
 * BeginCommand/EndCommand are executed once per received SQL query.
 *
 * CreateDestReceiver returns a receiver object appropriate to the specified
 * destination.  The executor, as well as utility statements that can return
 * tuples, are passed the resulting DestReceiver* pointer.  Each executor run
 * or utility execution calls the receiver's rStartup method, then the
 * receiveSlot method (zero or more times), then the rShutdown method.
 * The same receiver object may be re-used multiple times; eventually it is
 * destroyed by calling its rDestroy method.
 *
 * In some cases, receiver objects require additional parameters that must
 * be passed to them after calling CreateDestReceiver.  Since the set of
 * parameters varies for different receiver types, this is not handled by
 * this module, but by direct calls from the calling code to receiver type
 * specific functions.
 *
 * The DestReceiver object returned by CreateDestReceiver may be a statically
 * allocated object (for destination types that require no local state),
 * in which case rDestroy is a no-op.  Alternatively it can be a palloc'd
 * object that has DestReceiver as its first field and contains additional
 * fields (see printtup.c for an example).  These additional fields are then
 * accessible to the DestReceiver functions by casting the DestReceiver*
 * pointer passed to them.  The palloc'd object is pfree'd by the rDestroy
 * method.  Note that the caller of CreateDestReceiver should take care to
 * do so in a memory context that is long-lived enough for the receiver
 * object not to disappear while still needed.
 *
 * Special provision: None_Receiver is a permanently available receiver
 * object for the DestNone destination.  This avoids useless creation/destroy
 * calls in portal and cursor manipulations.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/dest.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DEST_H
#define DEST_H

#include "executor/tuptable.h"


/* buffer size to use for command completion tags */
#define COMPLETION_TAG_BUFSIZE	64


/* ----------------
 *		CommandDest is a simplistic means of identifying the desired
 *		destination.  Someday this will probably need to be improved.
 *
 * Note: only the values DestNone, DestDebug, DestRemote are legal for the
 * global variable whereToSendOutput.   The other values may be used
 * as the destination for individual commands.
 * ----------------
 */
typedef enum
{
	DestNone,					/* results are discarded */
	DestDebug,					/* results go to debugging output */
	DestRemote,					/* results sent to frontend process */
	DestRemoteExecute,			/* sent to frontend, in Execute command */
	DestSPI,					/* results sent to SPI manager */
	DestTuplestore,				/* results sent to Tuplestore */
	DestIntoRel,				/* results sent to relation (SELECT INTO) */
	DestCopyOut,				/* results sent to COPY TO code */
	DestSQLFunction,			/* results sent to SQL-language func mgr */
	DestTransientRel			/* results sent to transient relation */
} CommandDest;

/* ----------------
 *		DestReceiver is a base type for destination-specific local state.
 *		In the simplest cases, there is no state info, just the function
 *		pointers that the executor must call.
 *
 * Note: the receiveSlot routine must be passed a slot containing a TupleDesc
 * identical to the one given to the rStartup routine.
 * ----------------
 */
typedef struct _DestReceiver DestReceiver;

struct _DestReceiver
{
	/* Called for each tuple to be output: */
	void		(*receiveSlot) (TupleTableSlot *slot,
											DestReceiver *self);
	/* Per-executor-run initialization and shutdown: */
	void		(*rStartup) (DestReceiver *self,
										 int operation,
										 TupleDesc typeinfo);
	void		(*rShutdown) (DestReceiver *self);
	/* Destroy the receiver object itself (if dynamically allocated) */
	void		(*rDestroy) (DestReceiver *self);
	/* CommandDest code for this receiver */
	CommandDest mydest;
	/* Private fields might appear beyond this point... */
};

extern DestReceiver *None_Receiver;		/* permanent receiver for DestNone */

/* The primary destination management functions */

extern void BeginCommand(const char *commandTag, CommandDest dest);
extern DestReceiver *CreateDestReceiver(CommandDest dest);
extern void EndCommand(const char *commandTag, CommandDest dest);

/* Additional functions that go with destination management, more or less. */

extern void NullCommand(CommandDest dest);
extern void ReadyForQuery(CommandDest dest);

#endif   /* DEST_H */
