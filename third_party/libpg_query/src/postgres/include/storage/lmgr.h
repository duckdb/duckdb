/*-------------------------------------------------------------------------
 *
 * lmgr.h
 *	  POSTGRES lock manager definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/lmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LMGR_H
#define LMGR_H

#include "lib/stringinfo.h"
#include "storage/itemptr.h"
#include "storage/lock.h"
#include "utils/rel.h"


/* XactLockTableWait operations */
typedef enum XLTW_Oper
{
	XLTW_None,
	XLTW_Update,
	XLTW_Delete,
	XLTW_Lock,
	XLTW_LockUpdated,
	XLTW_InsertIndex,
	XLTW_InsertIndexUnique,
	XLTW_FetchUpdated,
	XLTW_RecheckExclusionConstr
} XLTW_Oper;

extern void RelationInitLockInfo(Relation relation);

/* Lock a relation */
extern void LockRelationOid(Oid relid, LOCKMODE lockmode);
extern bool ConditionalLockRelationOid(Oid relid, LOCKMODE lockmode);
extern void UnlockRelationId(LockRelId *relid, LOCKMODE lockmode);
extern void UnlockRelationOid(Oid relid, LOCKMODE lockmode);

extern void LockRelation(Relation relation, LOCKMODE lockmode);
extern bool ConditionalLockRelation(Relation relation, LOCKMODE lockmode);
extern void UnlockRelation(Relation relation, LOCKMODE lockmode);
extern bool LockHasWaitersRelation(Relation relation, LOCKMODE lockmode);

extern void LockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode);
extern void UnlockRelationIdForSession(LockRelId *relid, LOCKMODE lockmode);

/* Lock a relation for extension */
extern void LockRelationForExtension(Relation relation, LOCKMODE lockmode);
extern void UnlockRelationForExtension(Relation relation, LOCKMODE lockmode);

/* Lock a page (currently only used within indexes) */
extern void LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);
extern bool ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);
extern void UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode);

/* Lock a tuple (see heap_lock_tuple before assuming you understand this) */
extern void LockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode);
extern bool ConditionalLockTuple(Relation relation, ItemPointer tid,
					 LOCKMODE lockmode);
extern void UnlockTuple(Relation relation, ItemPointer tid, LOCKMODE lockmode);

/* Lock an XID (used to wait for a transaction to finish) */
extern void XactLockTableInsert(TransactionId xid);
extern void XactLockTableDelete(TransactionId xid);
extern void XactLockTableWait(TransactionId xid, Relation rel,
				  ItemPointer ctid, XLTW_Oper oper);
extern bool ConditionalXactLockTableWait(TransactionId xid);

/* Lock VXIDs, specified by conflicting locktags */
extern void WaitForLockers(LOCKTAG heaplocktag, LOCKMODE lockmode);
extern void WaitForLockersMultiple(List *locktags, LOCKMODE lockmode);

/* Lock an XID for tuple insertion (used to wait for an insertion to finish) */
extern uint32 SpeculativeInsertionLockAcquire(TransactionId xid);
extern void SpeculativeInsertionLockRelease(TransactionId xid);
extern void SpeculativeInsertionWait(TransactionId xid, uint32 token);

/* Lock a general object (other than a relation) of the current database */
extern void LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
				   LOCKMODE lockmode);
extern void UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
					 LOCKMODE lockmode);

/* Lock a shared-across-databases object (other than a relation) */
extern void LockSharedObject(Oid classid, Oid objid, uint16 objsubid,
				 LOCKMODE lockmode);
extern void UnlockSharedObject(Oid classid, Oid objid, uint16 objsubid,
				   LOCKMODE lockmode);

extern void LockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid,
						   LOCKMODE lockmode);
extern void UnlockSharedObjectForSession(Oid classid, Oid objid, uint16 objsubid,
							 LOCKMODE lockmode);

/* Describe a locktag for error messages */
extern void DescribeLockTag(StringInfo buf, const LOCKTAG *tag);

#endif   /* LMGR_H */
