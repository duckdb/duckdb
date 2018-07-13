/*-------------------------------------------------------------------------
 *
 * predicate.h
 *	  POSTGRES public predicate locking definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/predicate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PREDICATE_H
#define PREDICATE_H

#include "utils/relcache.h"
#include "utils/snapshot.h"


/*
 * GUC variables
 */
extern int	max_predicate_locks_per_xact;


/* Number of SLRU buffers to use for predicate locking */
#define NUM_OLDSERXID_BUFFERS	16


/*
 * function prototypes
 */

/* housekeeping for shared memory predicate lock structures */
extern void InitPredicateLocks(void);
extern Size PredicateLockShmemSize(void);

extern void CheckPointPredicate(void);

/* predicate lock reporting */
extern bool PageIsPredicateLocked(Relation relation, BlockNumber blkno);

/* predicate lock maintenance */
extern Snapshot GetSerializableTransactionSnapshot(Snapshot snapshot);
extern void SetSerializableTransactionSnapshot(Snapshot snapshot,
								   TransactionId sourcexid);
extern void RegisterPredicateLockingXid(TransactionId xid);
extern void PredicateLockRelation(Relation relation, Snapshot snapshot);
extern void PredicateLockPage(Relation relation, BlockNumber blkno, Snapshot snapshot);
extern void PredicateLockTuple(Relation relation, HeapTuple tuple, Snapshot snapshot);
extern void PredicateLockPageSplit(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void PredicateLockPageCombine(Relation relation, BlockNumber oldblkno, BlockNumber newblkno);
extern void TransferPredicateLocksToHeapRelation(Relation relation);
extern void ReleasePredicateLocks(bool isCommit);

/* conflict detection (may also trigger rollback) */
extern void CheckForSerializableConflictOut(bool valid, Relation relation, HeapTuple tuple,
								Buffer buffer, Snapshot snapshot);
extern void CheckForSerializableConflictIn(Relation relation, HeapTuple tuple, Buffer buffer);
extern void CheckTableForSerializableConflictIn(Relation relation);

/* final rollback checking */
extern void PreCommit_CheckForSerializationFailure(void);

/* two-phase commit support */
extern void AtPrepare_PredicateLocks(void);
extern void PostPrepare_PredicateLocks(TransactionId xid);
extern void PredicateLockTwoPhaseFinish(TransactionId xid, bool isCommit);
extern void predicatelock_twophase_recover(TransactionId xid, uint16 info,
							   void *recdata, uint32 len);

#endif   /* PREDICATE_H */
