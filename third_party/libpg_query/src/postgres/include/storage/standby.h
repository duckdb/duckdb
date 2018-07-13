/*-------------------------------------------------------------------------
 *
 * standby.h
 *	  Definitions for hot standby mode.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/standby.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STANDBY_H
#define STANDBY_H

#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "storage/lock.h"
#include "storage/procsignal.h"
#include "storage/relfilenode.h"

/* User-settable GUC parameters */
extern int	vacuum_defer_cleanup_age;
extern int	max_standby_archive_delay;
extern int	max_standby_streaming_delay;

extern void InitRecoveryTransactionEnvironment(void);
extern void ShutdownRecoveryTransactionEnvironment(void);

extern void ResolveRecoveryConflictWithSnapshot(TransactionId latestRemovedXid,
									RelFileNode node);
extern void ResolveRecoveryConflictWithTablespace(Oid tsid);
extern void ResolveRecoveryConflictWithDatabase(Oid dbid);

extern void ResolveRecoveryConflictWithBufferPin(void);
extern void CheckRecoveryConflictDeadlock(void);
extern void StandbyDeadLockHandler(void);
extern void StandbyTimeoutHandler(void);

/*
 * Standby Rmgr (RM_STANDBY_ID)
 *
 * Standby recovery manager exists to perform actions that are required
 * to make hot standby work. That includes logging AccessExclusiveLocks taken
 * by transactions and running-xacts snapshots.
 */
extern void StandbyAcquireAccessExclusiveLock(TransactionId xid, Oid dbOid, Oid relOid);
extern void StandbyReleaseLockTree(TransactionId xid,
					   int nsubxids, TransactionId *subxids);
extern void StandbyReleaseAllLocks(void);
extern void StandbyReleaseOldLocks(int nxids, TransactionId *xids);

/*
 * XLOG message types
 */
#define XLOG_STANDBY_LOCK			0x00
#define XLOG_RUNNING_XACTS			0x10

typedef struct xl_standby_locks
{
	int			nlocks;			/* number of entries in locks array */
	xl_standby_lock locks[FLEXIBLE_ARRAY_MEMBER];
} xl_standby_locks;

/*
 * When we write running xact data to WAL, we use this structure.
 */
typedef struct xl_running_xacts
{
	int			xcnt;			/* # of xact ids in xids[] */
	int			subxcnt;		/* # of subxact ids in xids[] */
	bool		subxid_overflow;	/* snapshot overflowed, subxids missing */
	TransactionId nextXid;		/* copy of ShmemVariableCache->nextXid */
	TransactionId oldestRunningXid;		/* *not* oldestXmin */
	TransactionId latestCompletedXid;	/* so we can set xmax */

	TransactionId xids[FLEXIBLE_ARRAY_MEMBER];
} xl_running_xacts;

#define MinSizeOfXactRunningXacts offsetof(xl_running_xacts, xids)


/* Recovery handlers for the Standby Rmgr (RM_STANDBY_ID) */
extern void standby_redo(XLogReaderState *record);
extern void standby_desc(StringInfo buf, XLogReaderState *record);
extern const char *standby_identify(uint8 info);

/*
 * Declarations for GetRunningTransactionData(). Similar to Snapshots, but
 * not quite. This has nothing at all to do with visibility on this server,
 * so this is completely separate from snapmgr.c and snapmgr.h.
 * This data is important for creating the initial snapshot state on a
 * standby server. We need lots more information than a normal snapshot,
 * hence we use a specific data structure for our needs. This data
 * is written to WAL as a separate record immediately after each
 * checkpoint. That means that wherever we start a standby from we will
 * almost immediately see the data we need to begin executing queries.
 */

typedef struct RunningTransactionsData
{
	int			xcnt;			/* # of xact ids in xids[] */
	int			subxcnt;		/* # of subxact ids in xids[] */
	bool		subxid_overflow;	/* snapshot overflowed, subxids missing */
	TransactionId nextXid;		/* copy of ShmemVariableCache->nextXid */
	TransactionId oldestRunningXid;		/* *not* oldestXmin */
	TransactionId latestCompletedXid;	/* so we can set xmax */

	TransactionId *xids;		/* array of (sub)xids still running */
} RunningTransactionsData;

typedef RunningTransactionsData *RunningTransactions;

extern void LogAccessExclusiveLock(Oid dbOid, Oid relOid);
extern void LogAccessExclusiveLockPrepare(void);

extern XLogRecPtr LogStandbySnapshot(void);

#endif   /* STANDBY_H */
